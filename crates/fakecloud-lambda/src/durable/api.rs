//! The durable execution operations of the 2025-12-01 Lambda API, and the
//! `Invoke` path that starts executions.
//!
//! Error codes and messages follow the recorded AWS responses: an unknown
//! execution is `ResourceNotFoundException` ("Durable Execution does not
//! exist") for Get/History/Stop but `Invalid checkpoint token` for the two
//! operations the SDK calls with a token; a callback id that does not
//! decode is `Invalid callback id`; one naming no open callback is
//! `CallbackTimeoutException`.

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::{InvocationType, LambdaService};
use crate::state::{LambdaState, SharedLambdaState};

use super::engine::{self, CallbackOutcome};
use super::model::{self, callback_execution_arn, strip_event_data, TimeFormat};
use super::runner::{self, DurableHub, DurableSettings, StartOutcome};
use super::validate::invalid_parameter;

/// The answer for a synchronous caller whose execution vanished (a reset of
/// fakecloud's state) before it ended. A synchronous caller otherwise waits
/// until the execution ends, which its `ExecutionTimeout` bounds: AWS keeps
/// the request open for the whole execution (recorded: a 1200 s execution
/// answered after 1200 s, past the documented 15 minutes).
fn unavailable_message(arn: &str) -> String {
    format!("Durable execution {arn} is no longer available")
}

fn not_found() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        "Durable Execution does not exist",
    )
}

fn callback_closed() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "CallbackTimeoutException",
        "The callback is either timed out or already completed",
    )
}

fn check_len(field: &str, v: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    let len = v.chars().count();
    if len < min || len > max {
        return Err(invalid_parameter(format!(
            "{field} length must be in [{min},{max}], got {len}"
        )));
    }
    Ok(())
}

fn query_bool(req: &AwsRequest, key: &str) -> Option<bool> {
    req.query_params
        .get(key)
        .map(|v| v.eq_ignore_ascii_case("true"))
}

fn max_items(req: &AwsRequest) -> Result<usize, AwsServiceError> {
    match req.query_params.get("MaxItems") {
        None => Ok(0),
        Some(s) => {
            let n: i64 = s
                .parse()
                .map_err(|_| invalid_parameter("MaxItems must be int"))?;
            if !(0..=1000).contains(&n) {
                return Err(invalid_parameter(format!(
                    "MaxItems must be in [0,1000], got {n}"
                )));
            }
            Ok(n as usize)
        }
    }
}

fn with_execution<T>(
    state: &SharedLambdaState,
    req: &AwsRequest,
    arn: &str,
    f: impl FnOnce(&super::model::DurableExecution) -> T,
) -> Option<T> {
    let accounts = state.read();
    accounts
        .regional(&req.account_id, &req.region)
        .and_then(|st| st.durable_executions.get(arn))
        .map(f)
}

/// The KMS error a read or write of `exec`'s data answers, when its
/// customer managed key is no longer usable.
fn key_error(
    hub: &DurableHub,
    req: &AwsRequest,
    exec: &super::model::DurableExecution,
) -> Option<AwsServiceError> {
    super::config::execution_key_error(hub.kms().as_deref(), &req.account_id, &req.region, exec)
}

/// `GetDurableExecution`.
pub(crate) fn get_durable_execution(
    hub: &DurableHub,
    req: &AwsRequest,
    arn: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("DurableExecutionArn", arn, 1, 1024)?;
    let include = query_bool(req, "IncludeExecutionData").unwrap_or(true);
    // Metadata needs no key; the execution's data does.
    with_execution(hub.state(), req, arn, |e| {
        match key_error(hub, req, e).filter(|_| include) {
            Some(err) => Err(err),
            None => Ok(e.detail_json(include)),
        }
    })
    .ok_or_else(not_found)?
    .map(AwsResponse::ok_json)
}

/// `GetDurableExecutionHistory`.
pub(crate) fn get_durable_execution_history(
    hub: &DurableHub,
    req: &AwsRequest,
    arn: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let state = hub.state();
    check_len("DurableExecutionArn", arn, 1, 1024)?;
    let include = query_bool(req, "IncludeExecutionData").unwrap_or(false);
    let reverse = query_bool(req, "ReverseOrder").unwrap_or(false);
    let max = max_items(req)?;
    let events = with_execution(state, req, arn, |e| {
        match key_error(hub, req, e).filter(|_| include) {
            Some(err) => Err(err),
            None => Ok(e.events.clone()),
        }
    })
    .ok_or_else(not_found)??;
    // Event ids only grow, so a marker keeps its place while events arrive.
    let keyed: Vec<(u64, Value)> = events
        .into_iter()
        .map(|e| (e["EventId"].as_u64().unwrap_or_default(), e))
        .collect();
    let (page, next) = engine::page_by_key(
        keyed,
        reverse,
        req.query_params.get("Marker").map(String::as_str),
        max,
    )?;
    let events: Vec<Value> = if include {
        page
    } else {
        page.iter().map(strip_event_data).collect()
    };
    let mut body = json!({ "Events": events });
    if let Some(n) = next {
        body["NextMarker"] = json!(n);
    }
    Ok(AwsResponse::ok_json(body))
}

/// `GetDurableExecutionState`.
pub(crate) fn get_durable_execution_state(
    state: &SharedLambdaState,
    req: &AwsRequest,
    arn: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("DurableExecutionArn", arn, 1, 1024)?;
    let token = req
        .query_params
        .get("CheckpointToken")
        .map(String::as_str)
        .unwrap_or_default();
    if !token.is_empty() {
        check_len("CheckpointToken", token, 1, 2048)?;
    }
    let max = max_items(req)?;
    let marker = req.query_params.get("Marker").map(String::as_str);
    with_execution(state, req, arn, |e| {
        engine::state_page(e, token, marker, max)
    })
    .unwrap_or_else(|| Err(engine::invalid_token()))
    .map(AwsResponse::ok_json)
}

/// `CheckpointDurableExecution`.
pub(crate) fn checkpoint_durable_execution(
    hub: &DurableHub,
    req: &AwsRequest,
    arn: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("DurableExecutionArn", arn, 1, 1024)?;
    let token = body
        .get("CheckpointToken")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !token.is_empty() {
        check_len("CheckpointToken", token, 1, 2048)?;
    }
    let client_token = body.get("ClientToken").and_then(Value::as_str);
    let updates: Vec<Value> = match body.get("Updates") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(a)) => a.clone(),
        Some(Value::Object(_)) => {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "SerializationException",
                "Start of structure or map found where not expected.",
            ))
        }
        Some(_) => {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "SerializationException",
                "Expected list or null",
            ))
        }
    };
    let result = {
        let mut accounts = hub.state().write();
        let exec = accounts
            .regional_get_mut(&req.account_id, &req.region)
            .and_then(|st| st.durable_executions.get_mut(arn))
            .ok_or_else(engine::invalid_token)?;
        engine::checkpoint(exec, token, client_token, &updates, model::now())?
    };
    hub.request_persist();
    // A chained invoke or a finished execution needs the scheduler.
    hub.kick();
    Ok(AwsResponse::ok_json(result))
}

/// `StopDurableExecution`.
pub(crate) fn stop_durable_execution(
    hub: &DurableHub,
    req: &AwsRequest,
    arn: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("DurableExecutionArn", arn, 1, 1024)?;
    let error = parse_error_object(&req.body)?;
    let at = {
        let mut accounts = hub.state().write();
        let exec = accounts
            .regional_get_mut(&req.account_id, &req.region)
            .and_then(|st| st.durable_executions.get_mut(arn))
            .ok_or_else(not_found)?;
        if let Some(err) = super::config::execution_key_error(
            hub.kms().as_deref(),
            &req.account_id,
            &req.region,
            exec,
        ) {
            return Err(err);
        }
        engine::stop(exec, error, model::now())
    };
    hub.request_persist();
    hub.kick();
    Ok(AwsResponse::ok_json(
        json!({ "StopTimestamp": model::ts(at, TimeFormat::Seconds) }),
    ))
}

/// The `ErrorObject` an operation carries as its HTTP payload; an empty
/// body is no error object.
fn parse_error_object(body: &[u8]) -> Result<Option<Value>, AwsServiceError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(m)) => Ok(Some(Value::Object(m))),
        Ok(Value::Null) => Ok(None),
        _ => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "SerializationException",
            "Start of structure or map expected",
        )),
    }
}

/// Smithy `CallbackId` `@pattern` `^[A-Za-z0-9+/]+={0,2}$`.
fn is_callback_id(id: &str) -> bool {
    let body = id.trim_end_matches('=');
    id.len() - body.len() <= 2
        && !body.is_empty()
        && body
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/')
}

fn validate_callback_id(callback_id: &str) -> Result<(), AwsServiceError> {
    use crate::extras::constraint_violation;
    let len = callback_id.chars().count();
    if len < 1 {
        return Err(constraint_violation(
            callback_id,
            "callbackId",
            "Member must have length greater than or equal to 1",
        ));
    }
    if len > 1024 {
        return Err(constraint_violation(
            callback_id,
            "callbackId",
            "Member must have length less than or equal to 1024",
        ));
    }
    if !is_callback_id(callback_id) {
        return Err(constraint_violation(
            callback_id,
            "callbackId",
            "Member must satisfy regular expression pattern: ^[A-Za-z0-9+/]+={0,2}$",
        ));
    }
    Ok(())
}

enum CallbackAction {
    Complete(CallbackOutcome),
    Heartbeat,
}

fn callback_call(
    hub: &DurableHub,
    req: &AwsRequest,
    callback_id: &str,
    action: CallbackAction,
) -> Result<AwsResponse, AwsServiceError> {
    validate_callback_id(callback_id)?;
    // A callback id that does not decode is refused with `"Type": null`,
    // where the API's other parameter errors carry `"Type": "User"`
    // (aws-round1 `callback-*-unknown`).
    let arn = callback_execution_arn(callback_id).ok_or_else(|| {
        AwsServiceError::aws_error_with_fields(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValueException",
            "Invalid callback id",
            vec![("Type".to_string(), "null".to_string())],
        )
    })?;
    let accepted = {
        let mut accounts = hub.state().write();
        let exec = accounts
            .regional_get_mut(&req.account_id, &req.region)
            .and_then(|st| st.durable_executions.get_mut(&arn))
            .ok_or_else(callback_closed)?;
        // Completing a callback stores its result or error under the key.
        if matches!(action, CallbackAction::Complete(_)) {
            if let Some(err) = super::config::execution_key_error(
                hub.kms().as_deref(),
                &req.account_id,
                &req.region,
                exec,
            ) {
                return Err(err);
            }
        }
        match action {
            CallbackAction::Complete(outcome) => {
                engine::complete_callback(exec, callback_id, outcome, model::now())
            }
            CallbackAction::Heartbeat => engine::heartbeat(exec, callback_id, model::now()),
        }
    };
    if !accepted {
        return Err(callback_closed());
    }
    hub.request_persist();
    hub.kick();
    Ok(AwsResponse::ok_json(json!({})))
}

/// `SendDurableExecutionCallbackSuccess`: the raw request body is the
/// callback's result.
pub(crate) fn send_callback_success(
    hub: &DurableHub,
    req: &AwsRequest,
    callback_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let result = (!req.body.is_empty()).then(|| String::from_utf8_lossy(&req.body).into_owned());
    callback_call(
        hub,
        req,
        callback_id,
        CallbackAction::Complete(CallbackOutcome::Succeeded(result)),
    )
}

/// `SendDurableExecutionCallbackFailure`: the request body is the
/// `ErrorObject`.
pub(crate) fn send_callback_failure(
    hub: &DurableHub,
    req: &AwsRequest,
    callback_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    validate_callback_id(callback_id)?;
    let error = parse_error_object(&req.body)?.unwrap_or_else(|| json!({}));
    callback_call(
        hub,
        req,
        callback_id,
        CallbackAction::Complete(CallbackOutcome::Failed(error)),
    )
}

/// `SendDurableExecutionCallbackHeartbeat`.
pub(crate) fn send_callback_heartbeat(
    hub: &DurableHub,
    req: &AwsRequest,
    callback_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    callback_call(hub, req, callback_id, CallbackAction::Heartbeat)
}

/// Every value of a repeated query key, in request order (`Statuses` is a
/// list member serialized as `Statuses=A&Statuses=B`).
fn repeated_query(req: &AwsRequest, key: &str) -> Vec<String> {
    let mut out = Vec::new();
    for pair in req.raw_query.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let k = percent_encoding::percent_decode_str(k).decode_utf8_lossy();
        if k == key {
            out.push(
                percent_encoding::percent_decode_str(&v.replace('+', " "))
                    .decode_utf8_lossy()
                    .into_owned(),
            );
        }
    }
    if out.is_empty() {
        if let Some(v) = req.query_params.get(key) {
            out.push(v.clone());
        }
    }
    out
}

fn parse_query_time(v: &str) -> Result<chrono::DateTime<chrono::Utc>, AwsServiceError> {
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(v) {
        return Ok(t.with_timezone(&chrono::Utc));
    }
    if let Ok(secs) = v.parse::<f64>() {
        if let Some(t) = chrono::DateTime::from_timestamp_millis((secs * 1000.0) as i64) {
            return Ok(t);
        }
    }
    Err(invalid_parameter(format!("Invalid timestamp: {v}")))
}

/// `ListDurableExecutionsByFunction`.
pub(crate) fn list_durable_executions_by_function(
    state: &SharedLambdaState,
    req: &AwsRequest,
    function_ref: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("FunctionName", function_ref, 1, 256)?;
    let name = crate::service::normalize_function_name(function_ref);
    let embedded_qualifier = crate::service::qualifier_from_function_ref(function_ref);
    let qualifier = req
        .query_params
        .get("Qualifier")
        .cloned()
        .or(embedded_qualifier);
    let execution_name = req.query_params.get("DurableExecutionName");
    if let Some(n) = execution_name {
        check_len("DurableExecutionName", n, 1, 64)?;
    }
    let max = max_items(req)?;
    let statuses = repeated_query(req, "Statuses");
    for s in &statuses {
        if !matches!(
            s.as_str(),
            "RUNNING" | "SUCCEEDED" | "FAILED" | "TIMED_OUT" | "STOPPED"
        ) {
            return Err(crate::extras::constraint_violation(
                s,
                "statuses",
                "Member must satisfy enum value set: [RUNNING, SUCCEEDED, FAILED, TIMED_OUT, STOPPED]",
            ));
        }
    }
    if statuses.len() > 1 {
        return Err(invalid_parameter("Cannot filter by more than one status"));
    }
    if execution_name.is_some() && qualifier.is_some() {
        return Err(invalid_parameter(
            "Cannot filter by DurableExecutionName when both FunctionName and Qualifier are provided",
        ));
    }
    let after = req
        .query_params
        .get("StartedAfter")
        .map(|v| parse_query_time(v))
        .transpose()?;
    let before = req
        .query_params
        .get("StartedBefore")
        .map(|v| parse_query_time(v))
        .transpose()?;
    let reverse = query_bool(req, "ReverseOrder").unwrap_or(false);

    let accounts = state.read();
    let empty = LambdaState::new(&req.account_id, &req.region);
    let st = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    if !st.functions.contains_key(&name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Function not found",
        ));
    }
    // A qualifier filters by the version the executions are pinned to; an
    // alias stands for the version it points at.
    let version_filter = qualifier.as_deref().map(|q| {
        if runner::is_version_qualifier(q) {
            q.to_string()
        } else {
            st.aliases
                .get(&format!("{name}:{q}"))
                .map(|a| a.function_version.clone())
                .unwrap_or_else(|| q.to_string())
        }
    });
    let matching: Vec<&super::model::DurableExecution> = st
        .durable_executions
        .values()
        .filter(|e| e.function_name == name)
        .filter(|e| version_filter.as_deref().is_none_or(|v| e.version == v))
        .filter(|e| execution_name.is_none_or(|n| &e.name == n))
        .filter(|e| statuses.is_empty() || statuses.contains(&e.status))
        .filter(|e| after.is_none_or(|t| e.start_timestamp > t))
        .filter(|e| before.is_none_or(|t| e.start_timestamp < t))
        .collect();
    // Newest first; ReverseOrder lists oldest first. The marker names the
    // last execution returned by (start, ARN), so executions started
    // between pages do not shift the next one.
    let keyed: Vec<((i64, String), Value)> = matching
        .iter()
        .map(|e| {
            (
                (e.start_timestamp.timestamp_millis(), e.arn.clone()),
                e.summary_json(),
            )
        })
        .collect();
    let (page, next) = engine::page_by_key(
        keyed,
        !reverse,
        req.query_params.get("Marker").map(String::as_str),
        max,
    )?;
    let mut body = json!({ "DurableExecutions": page });
    if let Some(n) = next {
        body["NextMarker"] = json!(n);
    }
    Ok(AwsResponse::ok_json(body))
}

fn header(req: &AwsRequest, name: &str) -> Option<String> {
    req.headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

fn set_header(resp: &mut AwsResponse, name: &'static str, value: &str) {
    if let Ok(v) = http::header::HeaderValue::from_str(value) {
        resp.headers
            .insert(http::header::HeaderName::from_static(name), v);
    }
}

impl LambdaService {
    /// `Invoke` of a durable function: start (or reattach to) a durable
    /// execution. `None` when the function is not durable or does not
    /// exist, so the regular invoke path answers.
    pub(crate) async fn invoke_durable(
        &self,
        req: &AwsRequest,
        function_name: &str,
        account_id: &str,
        qualifier: Option<&str>,
        invocation_type: InvocationType,
    ) -> Option<Result<AwsResponse, AwsServiceError>> {
        let region = req.region.as_str();
        let (settings, version) = {
            let accounts = self.state.read();
            let st = accounts.regional(account_id, region)?;
            let func = st.functions.get(function_name)?;
            let settings = DurableSettings::of(func)?;
            let Some(q) = qualifier else {
                return Some(Err(invalid_parameter(
                    "You cannot invoke a durable function using an unqualified ARN.",
                )));
            };
            if !runner::is_version_qualifier(q) {
                // An unknown alias falls through to the regular 404.
                st.aliases.get(&format!("{function_name}:{q}"))?;
            }
            let version = crate::service::resolve_qualifier_to_version(st, function_name, Some(q))
                .unwrap_or_else(|| "$LATEST".to_string());
            if !runner::version_exists(st, function_name, &version) {
                return Some(Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!(
                        "Function not found: {}",
                        crate::state::qualified_function_arn(region, account_id, function_name, q)
                    ),
                )));
            }
            // The pinned version's configuration governs its executions.
            let settings = runner::pinned_function(st, function_name, &version)
                .as_ref()
                .and_then(DurableSettings::of)
                .unwrap_or(settings);
            (settings, version)
        };
        if matches!(invocation_type, InvocationType::DryRun) {
            return Some(Ok(AwsResponse::json(StatusCode::NO_CONTENT, "")));
        }
        if !self.durable.has_runtime() {
            // As for any invoke: nothing can run the function.
            return Some(Err(AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServiceException",
                format!(
                    "Docker/Podman is required for Lambda execution but is not available. {}",
                    fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
                ),
            )));
        }
        let name = header(req, "x-amz-durable-execution-name");
        if let Some(n) = &name {
            if let Err(e) = validate_execution_name(n) {
                return Some(Err(e));
            }
        }
        let tenant = header(req, "x-amz-tenant-id");
        let input = if req.body.is_empty() {
            "{}".to_string()
        } else {
            String::from_utf8_lossy(&req.body).into_owned()
        };
        let outcome = {
            let mut accounts = self.state.write();
            let st = accounts.regional_mut(account_id, region);
            runner::start_execution(
                st,
                region,
                function_name,
                &version,
                &settings,
                name.as_deref(),
                &input,
                tenant.as_deref(),
                None,
            )
        };
        let arn = match outcome {
            StartOutcome::Started(arn) => {
                self.durable.request_persist();
                self.durable.kick();
                arn
            }
            StartOutcome::Existing(arn) => arn,
            StartOutcome::Conflict(arn) => {
                if matches!(invocation_type, InvocationType::Event) {
                    // An asynchronous start answers 202 with the execution
                    // already holding the name; nothing new starts.
                    arn
                } else {
                    return Some(Err(AwsServiceError::aws_error(
                        StatusCode::CONFLICT,
                        "DurableExecutionAlreadyStartedException",
                        format!("Execution already started: {arn}"),
                    )));
                }
            }
        };
        if matches!(invocation_type, InvocationType::Event) {
            let mut resp = AwsResponse::json(StatusCode::ACCEPTED, "");
            set_header(&mut resp, "x-amz-durable-execution-arn", &arn);
            return Some(Ok(resp));
        }
        let ended = self.durable.wait_for_end(account_id, region, &arn).await;
        let mut resp = match ended {
            Some(exec) => {
                let (body, function_error) = runner::sync_response_body(&exec);
                let mut resp = AwsResponse::json(StatusCode::OK, body);
                if function_error {
                    set_header(&mut resp, "x-amz-function-error", "Unhandled");
                }
                resp
            }
            None => {
                return Some(Err(AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ServiceException",
                    unavailable_message(&arn),
                )));
            }
        };
        set_header(&mut resp, "x-amz-durable-execution-arn", &arn);
        set_header(&mut resp, "x-amz-executed-version", &version);
        Some(Ok(resp))
    }
}

/// Start a durable execution of `function_ref` for a cross-service delivery
/// of `payload`, returning `(account, region, execution ARN)`. `None` for a
/// function that is not durable or does not exist, which the regular
/// delivery handles.
fn start_for_delivery(
    hub: &DurableHub,
    function_ref: &str,
    payload: &str,
    defaults: Option<(&str, &str)>,
) -> Option<Result<(String, String, String), String>> {
    let started = {
        let mut accounts = hub.state().write();
        let (default_account, default_region) = match defaults {
            Some((a, r)) => (a.to_string(), r.to_string()),
            None => (
                accounts.default_account_id().to_string(),
                accounts.region().to_string(),
            ),
        };
        let (account, region, name) =
            crate::state::function_location(function_ref, &default_account, &default_region);
        let (account, region, name) = (account.to_string(), region.to_string(), name.to_string());
        let st = accounts.regional_get_mut(&account, &region)?;
        let settings = DurableSettings::of(st.functions.get(&name)?)?;
        let Some(qualifier) = crate::service::qualifier_from_function_ref(function_ref) else {
            return Some(Err(
                "You cannot invoke a durable function using an unqualified ARN.".to_string(),
            ));
        };
        let version = crate::service::resolve_qualifier_to_version(st, &name, Some(&qualifier))
            .unwrap_or_else(|| "$LATEST".to_string());
        if !runner::version_exists(st, &name, &version) {
            return Some(Err(format!(
                "Function not found: {}",
                crate::state::qualified_function_arn(&region, &account, &name, &qualifier)
            )));
        }
        let settings = runner::pinned_function(st, &name, &version)
            .as_ref()
            .and_then(DurableSettings::of)
            .unwrap_or(settings);
        let input = if payload.is_empty() { "{}" } else { payload };
        let arn = match runner::start_execution(
            st, &region, &name, &version, &settings, None, input, None, None,
        ) {
            StartOutcome::Started(a) | StartOutcome::Existing(a) | StartOutcome::Conflict(a) => a,
        };
        (account, region, arn)
    };
    hub.request_persist();
    hub.kick();
    Some(Ok(started))
}

/// A synchronous cross-service delivery (event source mapping, ...) of
/// `payload` to `function_ref`: when the function is durable, start an
/// execution and wait for it to end, answering what a synchronous `Invoke`
/// would (the result, or the error envelope flagged as a function error).
/// `None` for a function that is not durable or does not exist, which the
/// regular delivery handles.
pub async fn invoke_for_delivery(
    hub: &DurableHub,
    function_ref: &str,
    payload: &str,
) -> Option<Result<fakecloud_core::delivery::LambdaInvocationOutput, String>> {
    let (account, region, arn) = match start_for_delivery(hub, function_ref, payload, None)? {
        Ok(started) => started,
        Err(e) => return Some(Err(e)),
    };
    Some(match hub.wait_for_end(&account, &region, &arn).await {
        Some(exec) => {
            let (payload, failed) = runner::sync_response_body(&exec);
            Ok(fakecloud_core::delivery::LambdaInvocationOutput {
                payload,
                function_error: failed.then(|| "Unhandled".to_string()),
            })
        }
        None => Err(unavailable_message(&arn)),
    })
}

/// An asynchronous cross-service delivery (SNS, S3 notifications,
/// EventBridge, Scheduler, ...) of `payload` to `function_ref`: when the
/// function is durable, start an execution and return, as an `Event`
/// `Invoke` does; the execution's own retries and timeout govern it from
/// there. `None` for a function that is not durable or does not exist,
/// which the regular asynchronous queue handles.
pub fn start_async_for_delivery(
    hub: &DurableHub,
    function_ref: &str,
    payload: &str,
) -> Option<Result<(), String>> {
    Some(start_for_delivery(hub, function_ref, payload, None)?.map(|_| ()))
}

/// [`start_async_for_delivery`] for a caller that resolves a bare function
/// name or a partial ARN in its own `account` and `region` (as AWS Config
/// resolves a rule's `SourceIdentifier` in the rule's account and region)
/// rather than the server's defaults.
pub fn start_async_for_delivery_in(
    hub: &DurableHub,
    function_ref: &str,
    payload: &str,
    account: &str,
    region: &str,
) -> Option<Result<(), String>> {
    Some(start_for_delivery(hub, function_ref, payload, Some((account, region)))?.map(|_| ()))
}

/// `DurableExecutionName`: `^[a-zA-Z0-9-_]+$`, 1 to 64 characters.
fn validate_execution_name(n: &str) -> Result<(), AwsServiceError> {
    use crate::extras::constraint_violation;
    let len = n.chars().count();
    if len > 64 {
        return Err(constraint_violation(
            n,
            "durableExecutionName",
            "Member must have length less than or equal to 64",
        ));
    }
    if len == 0
        || !n
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(constraint_violation(
            n,
            "durableExecutionName",
            "Member must satisfy regular expression pattern: [a-zA-Z0-9-_]+",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    const ACCOUNT: &str = "123456789012";

    fn shared() -> SharedLambdaState {
        Arc::new(RwLock::new(MultiAccountState::new(
            ACCOUNT,
            "us-east-1",
            "http://localhost:4566",
        )))
    }

    fn req(query: &[(&str, &str)], body: &[u8]) -> AwsRequest {
        let raw_query = query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        AwsRequest {
            service: "lambda".to_string(),
            action: "x".to_string(),
            method: Method::POST,
            raw_path: "/".to_string(),
            raw_query,
            path_segments: vec![],
            query_params: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
            headers: HeaderMap::new(),
            body: Bytes::copy_from_slice(body),
            body_stream: parking_lot::Mutex::new(None),
            account_id: ACCOUNT.to_string(),
            region: "us-east-1".to_string(),
            request_id: "req".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    /// A durable function `f` with one execution started through the
    /// public start path.
    fn seeded(name: Option<&str>) -> (DurableHub, String) {
        let state = shared();
        let hub = DurableHub::new(state.clone());
        let mut accounts = state.write();
        let st = accounts.regional_mut(ACCOUNT, "us-east-1");
        st.functions.insert(
            "f".into(),
            crate::state::LambdaFunction {
                function_name: "f".into(),
                durable_config: Some(json!({"ExecutionTimeout": 600})),
                ..Default::default()
            },
        );
        let settings = DurableSettings::of(&st.functions["f"]).unwrap();
        let arn = match runner::start_execution(
            st,
            "us-east-1",
            "f",
            "$LATEST",
            &settings,
            name,
            "{\"a\":1}",
            None,
            None,
        ) {
            StartOutcome::Started(a) => a,
            _ => unreachable!(),
        };
        drop(accounts);
        (hub, arn)
    }

    #[test]
    fn execution_names_are_idempotent_per_function() {
        let (hub, arn) = seeded(Some("order-1"));
        let mut accounts = hub.state().write();
        let st = accounts.regional_mut(ACCOUNT, "us-east-1");
        let settings = DurableSettings::of(&st.functions["f"]).unwrap();
        assert!(matches!(
            runner::start_execution(st, "us-east-1", "f", "$LATEST", &settings, Some("order-1"), "{\"a\":1}", None, None),
            StartOutcome::Existing(a) if a == arn
        ));
        assert!(matches!(
            runner::start_execution(st, "us-east-1", "f", "$LATEST", &settings, Some("order-1"), "{\"a\":2}", None, None),
            StartOutcome::Conflict(a) if a == arn
        ));
    }

    #[test]
    fn get_and_history_report_recorded_shapes() {
        let (hub, arn) = seeded(Some("order-1"));
        assert!(arn.starts_with(
            "arn:aws:lambda:us-east-1:123456789012:function:f:$LATEST/durable-execution/order-1/"
        ));
        let got = body(&get_durable_execution(&hub, &req(&[], b""), &arn).unwrap());
        assert_eq!(got["Status"], "RUNNING");
        assert_eq!(got["InputPayload"], "{\"a\":1}");
        assert_eq!(got["ExecutionDataIncluded"], true);
        assert_eq!(
            got["DurableConfig"],
            json!({"ExecutionTimeout":600,"RetentionPeriodInDays":14})
        );
        assert!(got["StartTimestamp"].is_f64() || got["StartTimestamp"].is_u64());
        let compact = body(
            &get_durable_execution(&hub, &req(&[("IncludeExecutionData", "false")], b""), &arn)
                .unwrap(),
        );
        assert!(compact.get("InputPayload").is_none());
        assert_eq!(compact["ExecutionDataIncluded"], false);

        let h = body(&get_durable_execution_history(&hub, &req(&[], b""), &arn).unwrap());
        assert_eq!(
            h["Events"][0]["ExecutionStartedDetails"],
            json!({"Input":{"Truncated":true},"ExecutionTimeout":600})
        );
        let h = body(
            &get_durable_execution_history(
                &hub,
                &req(&[("IncludeExecutionData", "true")], b""),
                &arn,
            )
            .unwrap(),
        );
        assert_eq!(
            h["Events"][0]["ExecutionStartedDetails"]["Input"],
            json!({"Truncated":false,"Payload":"{\"a\":1}"})
        );
    }

    #[test]
    fn unknown_executions_answer_per_operation() {
        let (hub, _) = seeded(None);
        let arn =
            "arn:aws:lambda:us-east-1:123456789012:function:f:$LATEST/durable-execution/nope/x";
        let r = req(&[], b"");
        for err in [
            get_durable_execution(&hub, &r, arn).err().unwrap(),
            get_durable_execution_history(&hub, &r, arn).err().unwrap(),
            stop_durable_execution(&hub, &r, arn).err().unwrap(),
        ] {
            assert_eq!(
                (err.code(), err.message().as_str()),
                (
                    "ResourceNotFoundException",
                    "Durable Execution does not exist"
                )
            );
        }
        for err in [
            get_durable_execution_state(
                hub.state(),
                &req(&[("CheckpointToken", "AAAA")], b""),
                arn,
            )
            .err()
            .unwrap(),
            checkpoint_durable_execution(&hub, &r, arn, &json!({"CheckpointToken":"AAAA"}))
                .err()
                .unwrap(),
        ] {
            assert_eq!(
                (err.code(), err.message().as_str()),
                ("InvalidParameterValueException", "Invalid checkpoint token")
            );
        }
    }

    #[test]
    fn callback_ids_are_resolved_through_their_execution() {
        let (hub, arn) = seeded(None);
        let err = send_callback_success(&hub, &req(&[], b"1"), "bm9wZQ==")
            .err()
            .unwrap();
        assert_eq!(
            (err.code(), err.message().as_str()),
            ("InvalidParameterValueException", "Invalid callback id")
        );
        // A well-formed id naming no open callback.
        let id = model::new_callback_id(&arn, "$LATEST");
        let err = send_callback_heartbeat(&hub, &req(&[], b""), &id)
            .err()
            .unwrap();
        assert_eq!(err.code(), "CallbackTimeoutException");
        let malformed = send_callback_failure(&hub, &req(&[], b""), "bad_id")
            .err()
            .unwrap();
        assert_eq!(malformed.code(), "ValidationException");

        // Open a callback the way the SDK does and complete it.
        let token = {
            let mut accounts = hub.state().write();
            let exec = accounts
                .regional_mut(ACCOUNT, "us-east-1")
                .durable_executions
                .get_mut(&arn)
                .unwrap();
            engine::begin_invocation(exec, "r1", model::now());
            exec.invocation.as_ref().unwrap().token.clone().unwrap()
        };
        let r = body(
            &checkpoint_durable_execution(
                &hub,
                &req(&[], b""),
                &arn,
                &json!({"CheckpointToken": token, "Updates":[{"Id":"cb","Type":"CALLBACK","Action":"START"}]}),
            )
            .unwrap(),
        );
        let cb = r["NewExecutionState"]["Operations"][0]["CallbackDetails"]["CallbackId"]
            .as_str()
            .unwrap()
            .to_string();
        send_callback_heartbeat(&hub, &req(&[], b""), &cb).unwrap();
        send_callback_failure(
            &hub,
            &req(&[], br#"{"ErrorMessage":"denied","ErrorType":"Rejected"}"#),
            &cb,
        )
        .unwrap();
        let again = send_callback_success(&hub, &req(&[], b"x"), &cb)
            .err()
            .unwrap();
        assert_eq!(
            (again.code(), again.message().as_str()),
            (
                "CallbackTimeoutException",
                "The callback is either timed out or already completed"
            )
        );
    }

    #[test]
    fn stop_reports_the_stop_time_and_is_repeatable() {
        let (hub, arn) = seeded(None);
        let first = body(
            &stop_durable_execution(
                &hub,
                &req(&[], br#"{"ErrorMessage":"operator stop"}"#),
                &arn,
            )
            .unwrap(),
        );
        let second = body(&stop_durable_execution(&hub, &req(&[], b""), &arn).unwrap());
        assert_eq!(first["StopTimestamp"], second["StopTimestamp"]);
        let got = body(&get_durable_execution(&hub, &req(&[], b""), &arn).unwrap());
        assert_eq!(got["Status"], "STOPPED");
        assert_eq!(got["Error"], json!({"ErrorMessage":"operator stop"}));
    }

    #[test]
    fn list_filters_and_validates() {
        let (hub, _) = seeded(Some("a"));
        {
            let mut accounts = hub.state().write();
            let st = accounts.regional_mut(ACCOUNT, "us-east-1");
            let settings = DurableSettings::of(&st.functions["f"]).unwrap();
            runner::start_execution(
                st,
                "us-east-1",
                "f",
                "$LATEST",
                &settings,
                Some("b"),
                "{}",
                None,
                None,
            );
        }
        let all =
            body(&list_durable_executions_by_function(hub.state(), &req(&[], b""), "f").unwrap());
        assert_eq!(all["DurableExecutions"].as_array().unwrap().len(), 2);
        let named = body(
            &list_durable_executions_by_function(
                hub.state(),
                &req(&[("DurableExecutionName", "b")], b""),
                "f",
            )
            .unwrap(),
        );
        assert_eq!(named["DurableExecutions"][0]["DurableExecutionName"], "b");
        let paged = body(
            &list_durable_executions_by_function(hub.state(), &req(&[("MaxItems", "1")], b""), "f")
                .unwrap(),
        );
        assert!(paged["NextMarker"].is_string());
        let none = body(
            &list_durable_executions_by_function(
                hub.state(),
                &req(&[("Qualifier", "1")], b""),
                "f",
            )
            .unwrap(),
        );
        assert_eq!(none["DurableExecutions"], json!([]));
        let arn_ref = body(
            &list_durable_executions_by_function(
                hub.state(),
                &req(&[], b""),
                "arn:aws:lambda:us-east-1:123456789012:function:f",
            )
            .unwrap(),
        );
        assert_eq!(arn_ref["DurableExecutions"].as_array().unwrap().len(), 2);
        let err = list_durable_executions_by_function(
            hub.state(),
            &req(&[("Statuses", "FAILED"), ("Statuses", "STOPPED")], b""),
            "f",
        )
        .err()
        .unwrap();
        assert_eq!(err.message(), "Cannot filter by more than one status");
        let err = list_durable_executions_by_function(
            hub.state(),
            &req(
                &[("Qualifier", "$LATEST"), ("DurableExecutionName", "a")],
                b"",
            ),
            "f",
        )
        .err()
        .unwrap();
        assert_eq!(
            err.message(),
            "Cannot filter by DurableExecutionName when both FunctionName and Qualifier are provided"
        );
        let err = list_durable_executions_by_function(hub.state(), &req(&[], b""), "missing")
            .err()
            .unwrap();
        assert_eq!(
            (err.code(), err.message().as_str()),
            ("ResourceNotFoundException", "Function not found")
        );
    }

    #[tokio::test]
    async fn deliveries_start_executions_only_for_qualified_durable_functions() {
        let (hub, _) = seeded(None);
        {
            let mut accounts = hub.state().write();
            accounts
                .regional_mut(ACCOUNT, "us-east-1")
                .functions
                .insert(
                    "plain".into(),
                    crate::state::LambdaFunction {
                        function_name: "plain".into(),
                        ..Default::default()
                    },
                );
        }
        assert!(invoke_for_delivery(&hub, "plain", "{}").await.is_none());
        assert!(invoke_for_delivery(&hub, "missing:$LATEST", "{}")
            .await
            .is_none());
        assert_eq!(
            invoke_for_delivery(
                &hub,
                "arn:aws:lambda:us-east-1:123456789012:function:f",
                "{}"
            )
            .await,
            Some(Err(
                "You cannot invoke a durable function using an unqualified ARN.".to_string()
            ))
        );
    }

    /// A delivery to a durable function whose execution fails reports the
    /// failure as a function error, so the integration retries or fails it
    /// instead of treating the error envelope as a result.
    #[tokio::test]
    async fn a_failed_execution_is_a_function_error_to_the_delivery() {
        let (hub, _) = seeded(None);
        hub.ensure_started();
        let out = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            invoke_for_delivery(&hub, "f:$LATEST", "{}"),
        )
        .await
        .expect("execution ends")
        .expect("durable")
        .expect("ran");
        assert_eq!(out.function_error.as_deref(), Some("Unhandled"));
        let payload: Value = serde_json::from_slice(&out.payload).unwrap();
        assert!(payload.get("errorMessage").is_some(), "{payload}");
    }

    #[tokio::test]
    async fn an_async_delivery_starts_an_execution_without_waiting() {
        let (hub, first) = seeded(None);
        assert_eq!(
            start_async_for_delivery(&hub, "plain-missing:$LATEST", "{}"),
            None
        );
        assert_eq!(
            start_async_for_delivery(&hub, "f", "{}"),
            Some(Err(
                "You cannot invoke a durable function using an unqualified ARN.".to_string()
            ))
        );
        assert_eq!(
            start_async_for_delivery(&hub, "f:$LATEST", "{\"b\":2}"),
            Some(Ok(()))
        );
        let accounts = hub.state().read();
        let execs = &accounts
            .regional(ACCOUNT, "us-east-1")
            .unwrap()
            .durable_executions;
        let started: Vec<_> = execs.keys().filter(|a| **a != first).collect();
        assert_eq!(started.len(), 1, "{execs:?}");
        assert_eq!(execs[started[0]].input_payload, "{\"b\":2}");
    }

    #[tokio::test]
    async fn deleting_a_durable_function_is_asynchronous() {
        let (hub, _) = seeded(None);
        let svc = crate::service::LambdaService::new(hub.state().clone());
        {
            let mut accounts = hub.state().write();
            accounts
                .regional_mut(ACCOUNT, "us-east-1")
                .functions
                .insert(
                    "plain".into(),
                    crate::state::LambdaFunction {
                        function_name: "plain".into(),
                        ..Default::default()
                    },
                );
        }
        let durable = svc
            .delete_function("f", ACCOUNT, "us-east-1", None)
            .unwrap();
        assert_eq!(durable.status, StatusCode::ACCEPTED);
        let plain = svc
            .delete_function("plain", ACCOUNT, "us-east-1", None)
            .unwrap();
        assert_eq!(plain.status, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn durable_invokes_need_a_container_runtime() {
        let (hub, _) = seeded(None);
        let svc = crate::service::LambdaService::new(hub.state().clone());
        let resp = svc
            .invoke_durable(
                &req(&[], b"{}"),
                "f",
                ACCOUNT,
                Some("$LATEST"),
                InvocationType::Event,
            )
            .await
            .expect("f is durable");
        let err = resp.err().unwrap();
        assert_eq!(err.code(), "ServiceException");
        // Unqualified is still refused first, and DryRun needs no runtime.
        let err = svc
            .invoke_durable(&req(&[], b"{}"), "f", ACCOUNT, None, InvocationType::Event)
            .await
            .unwrap()
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidParameterValueException");
        let dry = svc
            .invoke_durable(
                &req(&[], b"{}"),
                "f",
                ACCOUNT,
                Some("$LATEST"),
                InvocationType::DryRun,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dry.status, StatusCode::NO_CONTENT);
    }

    #[test]
    fn list_and_history_markers_survive_inserts_between_pages() {
        let (hub, arn) = seeded(Some("a"));
        let start = |name: &str| {
            let mut accounts = hub.state().write();
            let st = accounts.regional_mut(ACCOUNT, "us-east-1");
            let settings = DurableSettings::of(&st.functions["f"]).unwrap();
            runner::start_execution(
                st,
                "us-east-1",
                "f",
                "$LATEST",
                &settings,
                Some(name),
                "{}",
                None,
                None,
            );
        };
        std::thread::sleep(std::time::Duration::from_millis(3));
        start("b");
        std::thread::sleep(std::time::Duration::from_millis(3));
        start("c");
        let names = |v: &Value| -> Vec<String> {
            v["DurableExecutions"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["DurableExecutionName"].as_str().unwrap().to_string())
                .collect()
        };
        let first = body(
            &list_durable_executions_by_function(hub.state(), &req(&[("MaxItems", "2")], b""), "f")
                .unwrap(),
        );
        assert_eq!(names(&first), ["c", "b"]);
        // A newer execution starts before the next page is read.
        std::thread::sleep(std::time::Duration::from_millis(3));
        start("d");
        let marker = first["NextMarker"].as_str().unwrap().to_string();
        let second = body(
            &list_durable_executions_by_function(
                hub.state(),
                &req(&[("MaxItems", "2"), ("Marker", &marker)], b""),
                "f",
            )
            .unwrap(),
        );
        assert_eq!(names(&second), ["a"]);
        assert!(second.get("NextMarker").is_none());
        let bad = list_durable_executions_by_function(
            hub.state(),
            &req(&[("Marker", "garbage")], b""),
            "f",
        )
        .err()
        .unwrap();
        assert_eq!(bad.message(), "Invalid marker");

        // History: events appended after a page do not shift the next page.
        let page1 = body(
            &get_durable_execution_history(&hub, &req(&[("MaxItems", "1")], b""), &arn).unwrap(),
        );
        {
            let mut accounts = hub.state().write();
            let exec = accounts
                .regional_mut(ACCOUNT, "us-east-1")
                .durable_executions
                .get_mut(&arn)
                .unwrap();
            engine::stop(exec, None, model::now());
        }
        let marker = page1["NextMarker"].as_str().map(str::to_string);
        assert!(marker.is_none(), "one event only before the stop");
        let reversed = body(
            &get_durable_execution_history(
                &hub,
                &req(&[("MaxItems", "1"), ("ReverseOrder", "true")], b""),
                &arn,
            )
            .unwrap(),
        );
        assert_eq!(reversed["Events"][0]["EventType"], "ExecutionStopped");
        let marker = reversed["NextMarker"].as_str().unwrap().to_string();
        let older = body(
            &get_durable_execution_history(
                &hub,
                &req(&[("ReverseOrder", "true"), ("Marker", &marker)], b""),
                &arn,
            )
            .unwrap(),
        );
        assert_eq!(older["Events"][0]["EventType"], "ExecutionStarted");
    }

    /// A KMS hook whose keys are in a fixed state.
    struct StubKms(Option<fakecloud_core::delivery::KmsKeyUnusable>);

    impl fakecloud_core::delivery::KmsHook for StubKms {
        fn encrypt(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: &[u8],
            _: &str,
            _: std::collections::HashMap<String, String>,
        ) -> Result<String, String> {
            Err("unused".into())
        }
        fn decrypt(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: std::collections::HashMap<String, String>,
        ) -> Result<Vec<u8>, String> {
            Err("unused".into())
        }
        fn check_service_key_use(
            &self,
            _: &str,
            _: &str,
            key_id: &str,
            _: &str,
            _: &str,
            _: &[&str],
            _: &std::collections::HashMap<String, String>,
        ) -> Result<String, fakecloud_core::delivery::KmsKeyUnusable> {
            match &self.0 {
                Some(r) => Err(r.clone()),
                None => Ok(key_id.to_string()),
            }
        }
    }

    #[tokio::test]
    async fn an_unusable_key_blocks_data_but_not_metadata() {
        use fakecloud_core::delivery::KmsKeyUnusable;
        let (hub, arn) = seeded(None);
        {
            let mut accounts = hub.state().write();
            accounts
                .regional_mut(ACCOUNT, "us-east-1")
                .durable_executions
                .get_mut(&arn)
                .unwrap()
                .kms_key_arn = Some("arn:aws:kms:us-east-1:123456789012:key/k".into());
        }
        hub.set_kms(Arc::new(StubKms(Some(KmsKeyUnusable::Disabled))));
        let err = get_durable_execution(&hub, &req(&[], b""), &arn)
            .err()
            .unwrap();
        assert_eq!(
            (err.status(), err.code()),
            (StatusCode::BAD_GATEWAY, "KMSDisabledException")
        );
        let meta = body(
            &get_durable_execution(&hub, &req(&[("IncludeExecutionData", "false")], b""), &arn)
                .unwrap(),
        );
        assert_eq!(
            meta["DurableConfig"]["KMSKeyArn"],
            "arn:aws:kms:us-east-1:123456789012:key/k"
        );
        assert!(get_durable_execution_history(&hub, &req(&[], b""), &arn).is_ok());
        let err = get_durable_execution_history(
            &hub,
            &req(&[("IncludeExecutionData", "true")], b""),
            &arn,
        )
        .err()
        .unwrap();
        assert_eq!(err.code(), "KMSDisabledException");
        let err = stop_durable_execution(&hub, &req(&[], b""), &arn)
            .err()
            .unwrap();
        assert_eq!(err.code(), "KMSDisabledException");
        hub.set_kms(Arc::new(StubKms(Some(KmsKeyUnusable::InvalidState(
            "PendingDeletion".into(),
        )))));
        let err = get_durable_execution(&hub, &req(&[], b""), &arn)
            .err()
            .unwrap();
        assert_eq!(err.code(), "KMSInvalidStateException");
        // Once the key is usable again, the data reads work.
        hub.set_kms(Arc::new(StubKms(None)));
        assert!(get_durable_execution(&hub, &req(&[], b""), &arn).is_ok());
    }

    #[test]
    fn configured_keys_are_checked_on_create() {
        use crate::durable::config::check_configured_key;
        use fakecloud_core::delivery::KmsKeyUnusable;
        let cfg = json!({"ExecutionTimeout": 60, "KMSKeyArn": "arn:aws:kms:us-east-1:123456789012:key/k"});
        let fa = "arn:aws:lambda:us-east-1:123456789012:function:f";
        for (reason, status, code) in [
            (
                KmsKeyUnusable::NotFound,
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
            ),
            (
                KmsKeyUnusable::Disabled,
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
            ),
            (
                KmsKeyUnusable::AccessDenied("arn:aws:kms:us-east-1:123456789012:key/k".into()),
                StatusCode::FORBIDDEN,
                "AccessDeniedException",
            ),
        ] {
            let hook = StubKms(Some(reason));
            let err = check_configured_key(Some(&hook), ACCOUNT, "us-east-1", fa, &cfg)
                .err()
                .unwrap();
            assert_eq!((err.status(), err.code()), (status, code));
        }
        assert!(check_configured_key(Some(&StubKms(None)), ACCOUNT, "us-east-1", fa, &cfg).is_ok());
        // No key, no check.
        let hook = StubKms(Some(KmsKeyUnusable::Disabled));
        assert!(check_configured_key(
            Some(&hook),
            ACCOUNT,
            "us-east-1",
            fa,
            &json!({"ExecutionTimeout": 60})
        )
        .is_ok());
    }

    /// Recorded (aws-round4-kms-alias.jsonl): an alias whose key policy
    /// refuses Lambda names the resolved key, a missing alias gets KMS's
    /// alias message, and an ARN outside the function's region is refused
    /// before KMS is asked.
    #[test]
    fn alias_and_foreign_key_arns_get_the_recorded_errors() {
        use crate::durable::config::check_configured_key;
        use fakecloud_core::delivery::KmsKeyUnusable;
        let fa = "arn:aws:lambda:us-east-1:123456789012:function:f";
        let cfg = |k: &str| json!({"ExecutionTimeout": 60, "KMSKeyArn": k});
        let resolved = "arn:aws:kms:us-east-1:123456789012:key/24a82b2a";
        let hook = StubKms(Some(KmsKeyUnusable::AccessDenied(resolved.into())));
        let err = check_configured_key(
            Some(&hook),
            ACCOUNT,
            "us-east-1",
            fa,
            &cfg("arn:aws:kms:us-east-1:123456789012:alias/aws/lambda"),
        )
        .err()
        .unwrap();
        assert_eq!(err.code(), "AccessDeniedException");
        assert_eq!(
            err.message(),
            format!("Lambda was unable to configure your durable function because KMS access was denied for key {resolved}. Please ensure your KMS key policy allows access to Lambda.")
        );
        let missing = "arn:aws:kms:us-east-1:123456789012:alias/fc-durable-no-such-alias";
        let hook = StubKms(Some(KmsKeyUnusable::NotFound));
        let err = check_configured_key(Some(&hook), ACCOUNT, "us-east-1", fa, &cfg(missing))
            .err()
            .unwrap();
        assert_eq!(
            err.message(),
            format!("Lambda was unable to configure your durable function because the specified KMS key {missing} was not found. Please check your KMS key settings. KMS Exception: NotFoundException KMS Message: Alias {missing} is not found.")
        );
        for foreign in [
            "arn:aws:s3:::bucket",
            "arn:aws:kms:eu-west-1:123456789012:key/k",
        ] {
            for hook in [
                None,
                Some(&StubKms(None) as &dyn fakecloud_core::delivery::KmsHook),
            ] {
                let err = check_configured_key(hook, ACCOUNT, "us-east-1", fa, &cfg(foreign))
                    .err()
                    .unwrap();
                assert_eq!(err.code(), "InvalidParameterValueException");
                assert_eq!(
                    err.message(),
                    format!("Lambda was unable to configure your durable function because the specified KMS key {foreign} is in a different region than the function. The KMS key must reside in the same region as the function.")
                );
            }
        }
    }

    #[test]
    fn execution_names_are_validated() {
        assert!(validate_execution_name("order-1_A").is_ok());
        assert!(validate_execution_name("bad name").is_err());
        assert!(validate_execution_name(&"x".repeat(65)).is_err());
    }
}
