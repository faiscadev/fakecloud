//! Lambda Workflows: Capacity Providers (2025-11-30 API) + Durable
//! Executions (2025-12-01 API). Adds 15 new operations on top of the
//! classic Lambda surface, exposing the AWS preview that lets functions
//! run inside customer-managed compute pools and execute long-running
//! state machines with callback hooks.

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    CapacityProvider, DurableExecution, DurableExecutionCallback, SharedLambdaState,
};

fn validation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        msg,
    )
}

fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "ResourceNotFoundException", msg)
}

fn check_len(field: &str, v: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if v.len() < min || v.len() > max {
        return Err(validation(format!(
            "{field} length must be in [{min},{max}], got {}",
            v.len()
        )));
    }
    Ok(())
}

fn arn_for_capacity_provider(region: &str, account: &str, name: &str) -> String {
    Arn::regional(
        "lambda",
        region,
        account,
        &format!("capacity-provider:{name}"),
    )
    .to_string()
}

fn capacity_provider_json(cp: &CapacityProvider) -> Value {
    json!({
        "CapacityProviderArn": cp.arn,
        "State": cp.state,
        "VpcConfig": cp.vpc_config,
        "PermissionsConfig": cp.permissions_config,
        "InstanceRequirements": cp.instance_requirements,
        "CapacityProviderScalingConfig": cp.scaling_config,
        "KmsKeyArn": cp.kms_key_arn,
        "LastModified": cp.last_modified.to_rfc3339(),
    })
}

pub(crate) fn create_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let cp = create_capacity_provider_in(state, &req.account_id, &req.region, body)?;
    Ok(AwsResponse::json_value(
        StatusCode::ACCEPTED,
        json!({ "CapacityProvider": capacity_provider_json(&cp) }),
    ))
}

/// `CreateCapacityProvider` in `account`/`region` from a request-shaped
/// `body`, shared with CloudFormation's `AWS::Lambda::CapacityProvider`.
pub fn create_capacity_provider_in(
    state: &SharedLambdaState,
    account: &str,
    region: &str,
    body: &Value,
) -> Result<CapacityProvider, AwsServiceError> {
    let name = body["CapacityProviderName"]
        .as_str()
        .ok_or_else(|| validation("CapacityProviderName is required"))?
        .to_string();
    check_len("CapacityProviderName", &name, 1, 140)?;
    let vpc = body
        .get("VpcConfig")
        .filter(|v| !v.is_null())
        .ok_or_else(|| validation("VpcConfig is required"))?
        .clone();
    let perms = body
        .get("PermissionsConfig")
        .filter(|v| !v.is_null())
        .ok_or_else(|| validation("PermissionsConfig is required"))?
        .clone();

    let arn = arn_for_capacity_provider(region, account, &name);

    let mut accts = state.write();
    let s = accts.regional_mut(account, region);
    if s.capacity_providers.contains_key(&name) {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "ResourceConflictException",
            format!("Capacity provider {name} already exists"),
        ));
    }

    let tags = body["Tags"]
        .as_object()
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let cp = CapacityProvider {
        name: name.clone(),
        arn,
        state: "Active".to_string(),
        vpc_config: vpc,
        permissions_config: perms,
        instance_requirements: body.get("InstanceRequirements").cloned(),
        scaling_config: body.get("CapacityProviderScalingConfig").cloned(),
        kms_key_arn: body["KmsKeyArn"].as_str().map(String::from),
        tags,
        last_modified: Utc::now(),
    };
    s.capacity_providers.insert(name, cp.clone());
    Ok(cp)
}

pub(crate) fn get_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("CapacityProviderName", name, 1, 140)?;
    let name = crate::capacity::capacity_provider_name_from_label(name)?;
    let accts = state.read();
    let empty = crate::state::LambdaState::new(&req.account_id, &req.region);
    let s = accts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let cp = s.capacity_providers.get(name).ok_or_else(|| {
        crate::capacity::provider_not_found(&arn_for_capacity_provider(
            &req.region,
            &req.account_id,
            name,
        ))
    })?;

    Ok(AwsResponse::ok_json(
        json!({ "CapacityProvider": capacity_provider_json(cp) }),
    ))
}

pub(crate) fn list_capacity_providers(
    state: &SharedLambdaState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    if let Some(s) = req.query_params.get("MaxItems") {
        let n: i64 = s.parse().map_err(|_| validation("MaxItems must be int"))?;
        if !(1..=50).contains(&n) {
            return Err(validation(format!("MaxItems must be in [1,50], got {n}")));
        }
    }
    if let Some(st) = req.query_params.get("State") {
        if !matches!(st.as_str(), "Pending" | "Active" | "Failed" | "Deleting") {
            return Err(validation(format!("State enum invalid: {st}")));
        }
    }
    let accts = state.read();
    let empty = crate::state::LambdaState::new(&req.account_id, &req.region);
    let s = accts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let providers: Vec<Value> = s
        .capacity_providers
        .values()
        .map(capacity_provider_json)
        .collect();

    Ok(AwsResponse::ok_json(json!({
        "CapacityProviders": providers,
    })))
}

pub(crate) fn update_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    name: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let cp = update_capacity_provider_in(state, &req.account_id, &req.region, name, body)?;
    Ok(AwsResponse::json_value(
        StatusCode::ACCEPTED,
        json!({ "CapacityProvider": capacity_provider_json(&cp) }),
    ))
}

/// `UpdateCapacityProvider` of the provider `name` (a name or ARN) in
/// `account`/`region`, shared with CloudFormation.
pub fn update_capacity_provider_in(
    state: &SharedLambdaState,
    account: &str,
    region: &str,
    name: &str,
    body: &Value,
) -> Result<CapacityProvider, AwsServiceError> {
    check_len("CapacityProviderName", name, 1, 140)?;
    let label = name;
    let name = crate::capacity::capacity_provider_name_from_label(label)?;
    let mut accts = state.write();
    let s = accts.regional_mut(account, region);
    let cp = s
        .capacity_providers
        .get_mut(name)
        .ok_or_else(|| crate::capacity::provider_not_found(label))?;
    if let Some(v) = body.get("VpcConfig").filter(|v| !v.is_null()) {
        cp.vpc_config = v.clone();
    }
    if let Some(v) = body.get("PermissionsConfig").filter(|v| !v.is_null()) {
        cp.permissions_config = v.clone();
    }
    if let Some(v) = body.get("InstanceRequirements") {
        cp.instance_requirements = if v.is_null() { None } else { Some(v.clone()) };
    }
    if let Some(v) = body.get("CapacityProviderScalingConfig") {
        cp.scaling_config = if v.is_null() { None } else { Some(v.clone()) };
    }
    if let Some(v) = body["KmsKeyArn"].as_str() {
        cp.kms_key_arn = Some(v.to_string());
    }
    cp.last_modified = Utc::now();
    Ok(cp.clone())
}

pub(crate) fn delete_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let deleted = delete_capacity_provider_in(state, &req.account_id, &req.region, name)?;
    Ok(AwsResponse::json_value(
        StatusCode::ACCEPTED,
        json!({ "CapacityProvider": capacity_provider_json(&deleted) }),
    ))
}

/// `DeleteCapacityProvider` of the provider `name` (a name or ARN) in
/// `account`/`region`, shared with CloudFormation. A provider still used by
/// a function version is refused with `ResourceConflictException`.
pub fn delete_capacity_provider_in(
    state: &SharedLambdaState,
    account: &str,
    region: &str,
    name: &str,
) -> Result<CapacityProvider, AwsServiceError> {
    check_len("CapacityProviderName", name, 1, 140)?;
    let label = name;
    let name = crate::capacity::capacity_provider_name_from_label(label)?;
    let mut accts = state.write();
    let arn = accts
        .regional(account, region)
        .and_then(|s| s.capacity_providers.get(name))
        .map(|cp| cp.arn.clone())
        .ok_or_else(|| crate::capacity::provider_not_found(label))?;
    // A capacity provider cannot be deleted while function versions use it.
    let in_use = crate::capacity::versions_using(&accts, &arn);
    if !in_use.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "ResourceConflictException",
            format!(
                "Capacity provider {arn} can't be deleted because it's in use by {} function version(s). Delete the function versions that use it first.",
                in_use.len()
            ),
        ));
    }
    let s = accts.regional_mut(account, region);
    let mut deleted = s
        .capacity_providers
        .remove(name)
        .ok_or_else(|| crate::capacity::provider_not_found(label))?;
    deleted.state = "Deleting".to_string();
    deleted.last_modified = Utc::now();
    Ok(deleted)
}

pub(crate) fn list_function_versions_by_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("CapacityProviderName", name, 1, 140)?;
    let label = name;
    let name = crate::capacity::capacity_provider_name_from_label(label)?;
    // `MaxItems` (1..50) is range-checked by the dispatcher.
    let max_items = req
        .query_params
        .get("MaxItems")
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(50);
    let accts = state.read();
    let cp = accts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.capacity_providers.get(name))
        .ok_or_else(|| crate::capacity::provider_not_found(label))?;
    let versions = crate::capacity::versions_using(&accts, &cp.arn);
    // `Marker` is the ARN of the last version of the previous page.
    let start = match req.query_params.get("Marker").filter(|m| !m.is_empty()) {
        Some(marker) => versions
            .iter()
            .position(|v| v.function_arn.as_str() > marker.as_str())
            .unwrap_or(versions.len()),
        None => 0,
    };
    let page: Vec<_> = versions.iter().skip(start).take(max_items).collect();
    let mut out = json!({
        "CapacityProviderArn": cp.arn,
        "FunctionVersions": page
            .iter()
            .map(|v| json!({"FunctionArn": v.function_arn, "State": v.state}))
            .collect::<Vec<_>>(),
    });
    if start + page.len() < versions.len() {
        if let Some(last) = page.last() {
            out["NextMarker"] = json!(last.function_arn);
        }
    }
    Ok(AwsResponse::ok_json(out))
}

// ─── Durable executions ───────────────────────────────────────────────────

fn execution_json(e: &DurableExecution) -> Value {
    json!({
        "DurableExecutionArn": e.arn,
        "FunctionName": e.function_name,
        "FunctionArn": e.function_arn,
        "Status": e.status,
        "Input": e.input,
        "StartedAt": e.started_at.to_rfc3339(),
        "StoppedAt": e.stopped_at.map(|t| t.to_rfc3339()),
        "LastModified": e.last_modified.to_rfc3339(),
    })
}

/// `ListDurableExecutionsByFunction` — we expose this so the harness can
/// drive the new endpoint with a function-name path label. Callers that
/// create executions by writing into state directly will see them here.
pub(crate) fn list_durable_executions_by_function(
    state: &SharedLambdaState,
    req: &AwsRequest,
    function_name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("FunctionName", function_name, 1, 170)?;
    {
        // Listing executions of a function that does not exist is a 404, as it
        // is for every other operation scoped to one function.
        let accts = state.read();
        let exists = accts
            .regional(&req.account_id, &req.region)
            .is_some_and(|s| s.functions.contains_key(function_name));
        if !exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Function not found: {function_name}"),
            ));
        }
    }
    if let Some(n) = req.query_params.get("DurableExecutionName") {
        check_len("DurableExecutionName", n, 1, 64)?;
    }
    if let Some(s) = req.query_params.get("MaxItems") {
        let n: i64 = s.parse().map_err(|_| validation("MaxItems must be int"))?;
        if !(0..=1000).contains(&n) {
            return Err(validation(format!("MaxItems must be in [0,1000], got {n}")));
        }
    }
    let accts = state.read();
    let empty = crate::state::LambdaState::new(&req.account_id, &req.region);
    let s = accts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let executions: Vec<Value> = s
        .durable_executions
        .values()
        .filter(|e| {
            e.function_name == function_name
                || e.function_arn
                    .ends_with(&format!(":function:{function_name}"))
        })
        .map(execution_json)
        .collect();

    Ok(AwsResponse::ok_json(json!({
        "DurableExecutions": executions,
    })))
}

fn ensure_execution<'a>(
    s: &'a crate::state::LambdaState,
    arn: &str,
) -> Result<&'a DurableExecution, AwsServiceError> {
    s.durable_executions
        .get(arn)
        .ok_or_else(|| not_found(format!("Durable execution {arn} not found")))
}

pub(crate) fn get_durable_execution(
    state: &SharedLambdaState,
    req: &AwsRequest,
    arn: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("DurableExecutionArn", arn, 1, 1024)?;
    let accts = state.read();
    let empty = crate::state::LambdaState::new(&req.account_id, &req.region);
    let s = accts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let exec = ensure_execution(s, arn)?;
    Ok(AwsResponse::ok_json(
        json!({ "DurableExecution": execution_json(exec) }),
    ))
}

pub(crate) fn get_durable_execution_history(
    state: &SharedLambdaState,
    req: &AwsRequest,
    arn: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("DurableExecutionArn", arn, 1, 1024)?;
    let accts = state.read();
    let empty = crate::state::LambdaState::new(&req.account_id, &req.region);
    let s = accts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let exec = ensure_execution(s, arn)?;
    Ok(AwsResponse::ok_json(json!({
        "Events": exec.history.clone(),
    })))
}

pub(crate) fn get_durable_execution_state(
    state: &SharedLambdaState,
    req: &AwsRequest,
    arn: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("DurableExecutionArn", arn, 1, 1024)?;
    // GetDurableExecutionState's Smithy declares only InvalidParameterValueException
    // for client errors (no ResourceNotFoundException), so map missing arn there.
    let accts = state.read();
    let empty = crate::state::LambdaState::new(&req.account_id, &req.region);
    let s = accts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let exec = s
        .durable_executions
        .get(arn)
        .ok_or_else(|| validation(format!("Durable execution {arn} not found")))?;
    Ok(AwsResponse::ok_json(json!({
        "State": exec.state.clone(),
    })))
}

pub(crate) fn checkpoint_durable_execution(
    state: &SharedLambdaState,
    req: &AwsRequest,
    arn: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("DurableExecutionArn", arn, 1, 1024)?;
    if let Some(token) = body.get("CheckpointToken").and_then(|v| v.as_str()) {
        check_len("CheckpointToken", token, 1, 2048)?;
    }
    // CheckpointDurableExecution Smithy doesn't declare ResourceNotFoundException;
    // map missing arn to InvalidParameterValueException.
    let mut accts = state.write();
    let s = accts.regional_mut(&req.account_id, &req.region);
    let exec = s
        .durable_executions
        .get_mut(arn)
        .ok_or_else(|| validation(format!("Durable execution {arn} not found")))?;
    if exec.status != "Running" && exec.status != "Pending" {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "ResourceConflictException",
            format!("Execution not active (status: {})", exec.status),
        ));
    }
    if let Some(s) = body.get("State") {
        exec.state = s.clone();
    }
    if let Some(evt) = body.get("Event") {
        exec.history.push(evt.clone());
    }
    exec.last_modified = Utc::now();
    Ok(AwsResponse::ok_json(json!({})))
}

pub(crate) fn stop_durable_execution(
    state: &SharedLambdaState,
    req: &AwsRequest,
    arn: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("DurableExecutionArn", arn, 1, 1024)?;
    let mut accts = state.write();
    let s = accts.regional_mut(&req.account_id, &req.region);
    let exec = s
        .durable_executions
        .get_mut(arn)
        .ok_or_else(|| not_found(format!("Durable execution {arn} not found")))?;
    if exec.status == "Stopped" {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "ResourceConflictException",
            "Execution already stopped",
        ));
    }
    exec.status = "Stopped".to_string();
    let now = Utc::now();
    exec.stopped_at = Some(now);
    exec.last_modified = now;
    Ok(AwsResponse::ok_json(json!({})))
}

/// The ARN shape of a durable execution, used to synthesize a parent for a
/// callback token that was never minted here.
fn execution_arn(region: &str, account: &str, id: &str) -> String {
    Arn::regional(
        "lambda",
        region,
        account,
        &format!("durable-execution/{id}"),
    )
    .to_string()
}

/// Smithy `CallbackId` `@pattern` `^[A-Za-z0-9+/]+={0,2}$`: a base64 token
/// (callback ids are minted as base64), padding only at the end.
fn is_callback_id(id: &str) -> bool {
    let body = id.trim_end_matches('=');
    id.len() - body.len() <= 2
        && !body.is_empty()
        && body
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/')
}

/// `CallbackId` constraints (`@length 1..1024`, base64 `@pattern`), refused
/// by input validation like every Lambda member constraint.
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

fn record_callback(
    state: &SharedLambdaState,
    req: &AwsRequest,
    callback_id: &str,
    outcome: &str,
) -> Result<AwsResponse, AwsServiceError> {
    validate_callback_id(callback_id)?;
    let mut accts = state.write();
    let s = accts.regional_mut(&req.account_id, &req.region);
    // AWS mints a callback token when a task suspends waiting on one, and
    // answers an id nobody handed out with ResourceNotFoundException. fakecloud
    // runs no suspending task, so it has no honest point at which to mint a
    // token — rejecting every id would leave these three operations permanently
    // unsatisfiable rather than merely strict. The parent execution arn is
    // synthesized for an unknown id instead, so the outcome is still recorded
    // and still observable.
    let execution_arn = s
        .durable_execution_callbacks
        .get(callback_id)
        .map(|cb| cb.execution_arn.clone())
        .unwrap_or_else(|| {
            execution_arn(&req.region, &req.account_id, &Uuid::new_v4().to_string())
        });
    s.durable_execution_callbacks.insert(
        callback_id.to_string(),
        DurableExecutionCallback {
            callback_id: callback_id.to_string(),
            execution_arn,
            outcome: outcome.to_string(),
            recorded_at: Utc::now(),
        },
    );
    Ok(AwsResponse::ok_json(json!({})))
}

pub(crate) fn send_callback_success(
    state: &SharedLambdaState,
    req: &AwsRequest,
    callback_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    record_callback(state, req, callback_id, "Succeeded")
}

pub(crate) fn send_callback_failure(
    state: &SharedLambdaState,
    req: &AwsRequest,
    callback_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    record_callback(state, req, callback_id, "Failed")
}

pub(crate) fn send_callback_heartbeat(
    state: &SharedLambdaState,
    req: &AwsRequest,
    callback_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    record_callback(state, req, callback_id, "Heartbeat")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::SharedLambdaState;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn shared() -> SharedLambdaState {
        Arc::new(RwLock::new(MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        )))
    }

    fn req() -> AwsRequest {
        AwsRequest {
            service: "lambda".to_string(),
            action: "x".to_string(),
            method: Method::POST,
            raw_path: "/".to_string(),
            raw_query: String::new(),
            path_segments: vec![],
            query_params: HashMap::new(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "req".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn create_then_get_capacity_provider_roundtrips_state() {
        let s = shared();
        let body = json!({
            "CapacityProviderName": "cp1",
            "VpcConfig": {"SubnetIds": ["sub"], "SecurityGroupIds": ["sg"]},
            "PermissionsConfig": {"RoleArn": "arn:aws:iam::123:role/r"}
        });
        let resp = create_capacity_provider(&s, &req(), &body).unwrap();
        assert_eq!(resp.status, StatusCode::ACCEPTED);
        let got = get_capacity_provider(&s, &req(), "cp1").unwrap();
        assert_eq!(got.status, StatusCode::OK);
    }

    #[test]
    fn create_capacity_provider_requires_vpc_and_perms() {
        let s = shared();
        let err = create_capacity_provider(&s, &req(), &json!({"CapacityProviderName": "x"}))
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn create_capacity_provider_duplicate_is_conflict() {
        let s = shared();
        let body = json!({
            "CapacityProviderName": "cp1",
            "VpcConfig": {},
            "PermissionsConfig": {}
        });
        create_capacity_provider(&s, &req(), &body).unwrap();
        let err = create_capacity_provider(&s, &req(), &body).err().unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn delete_capacity_provider_removes_state() {
        let s = shared();
        let body = json!({
            "CapacityProviderName": "cp1",
            "VpcConfig": {},
            "PermissionsConfig": {}
        });
        create_capacity_provider(&s, &req(), &body).unwrap();
        delete_capacity_provider(&s, &req(), "cp1").unwrap();
        let err = get_capacity_provider(&s, &req(), "cp1").err().unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn update_capacity_provider_patches_fields_and_bumps_last_modified() {
        let s = shared();
        let body = json!({
            "CapacityProviderName": "cp1",
            "VpcConfig": {"SubnetIds": []},
            "PermissionsConfig": {"RoleArn": "old"}
        });
        create_capacity_provider(&s, &req(), &body).unwrap();
        let prev_mod = s
            .read()
            .regional("123456789012", "us-east-1")
            .unwrap()
            .capacity_providers["cp1"]
            .last_modified;
        std::thread::sleep(std::time::Duration::from_millis(2));
        update_capacity_provider(
            &s,
            &req(),
            "cp1",
            &json!({"PermissionsConfig": {"RoleArn": "new"}}),
        )
        .unwrap();
        let state = s.read();
        let cp = &state
            .regional("123456789012", "us-east-1")
            .unwrap()
            .capacity_providers["cp1"];
        assert_eq!(cp.permissions_config["RoleArn"], "new");
        assert!(cp.last_modified > prev_mod);
    }

    fn seed_execution(s: &SharedLambdaState, arn: &str, function_name: &str, status: &str) {
        let mut accts = s.write();
        let st = accts.regional_mut("123456789012", "us-east-1");
        // An execution belongs to a function, and listing by function name
        // 404s when that function does not exist, so seed it too.
        if !st.functions.contains_key(function_name) {
            let f = crate::state::LambdaFunction {
                function_name: function_name.to_string(),
                ..Default::default()
            };
            st.functions.insert(function_name.to_string(), f);
        }
        st.durable_executions.insert(
            arn.to_string(),
            DurableExecution {
                arn: arn.to_string(),
                function_name: function_name.to_string(),
                function_arn: format!(
                    "arn:aws:lambda:us-east-1:123456789012:function:{function_name}"
                ),
                status: status.to_string(),
                input: json!({}),
                started_at: Utc::now(),
                stopped_at: None,
                last_modified: Utc::now(),
                history: vec![],
                state: json!({}),
            },
        );
    }

    #[test]
    fn get_durable_execution_returns_state() {
        let s = shared();
        let arn = "arn:aws:lambda:us-east-1:123456789012:durable-execution/e1";
        seed_execution(&s, arn, "fn1", "Running");
        let resp = get_durable_execution(&s, &req(), arn).unwrap();
        assert_eq!(resp.status, StatusCode::OK);
    }

    #[test]
    fn get_durable_execution_unknown_is_not_found() {
        let s = shared();
        let err = get_durable_execution(&s, &req(), "unknown").err().unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn checkpoint_durable_execution_appends_event_and_replaces_state() {
        let s = shared();
        let arn = "arn:aws:lambda:us-east-1:123456789012:durable-execution/e1";
        seed_execution(&s, arn, "fn1", "Running");
        let body = json!({"State": {"step": 2}, "Event": {"type": "Tick"}});
        checkpoint_durable_execution(&s, &req(), arn, &body).unwrap();
        let exec = s
            .read()
            .regional("123456789012", "us-east-1")
            .unwrap()
            .durable_executions[arn]
            .clone();
        assert_eq!(exec.state["step"], 2);
        assert_eq!(exec.history.len(), 1);
    }

    #[test]
    fn checkpoint_durable_execution_after_stop_is_conflict() {
        let s = shared();
        let arn = "arn:aws:lambda:us-east-1:123456789012:durable-execution/e1";
        seed_execution(&s, arn, "fn1", "Stopped");
        let err = checkpoint_durable_execution(&s, &req(), arn, &json!({}))
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn stop_durable_execution_idempotent_with_conflict_on_second_call() {
        let s = shared();
        let arn = "arn:aws:lambda:us-east-1:123456789012:durable-execution/e1";
        seed_execution(&s, arn, "fn1", "Running");
        stop_durable_execution(&s, &req(), arn).unwrap();
        let err = stop_durable_execution(&s, &req(), arn).err().unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn list_durable_executions_filters_by_function_name() {
        let s = shared();
        seed_execution(
            &s,
            "arn:aws:lambda:us-east-1:123456789012:durable-execution/e1",
            "alpha",
            "Running",
        );
        seed_execution(
            &s,
            "arn:aws:lambda:us-east-1:123456789012:durable-execution/e2",
            "beta",
            "Running",
        );
        let resp = list_durable_executions_by_function(&s, &req(), "alpha").unwrap();
        let text = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        let v: Value = serde_json::from_str(text).unwrap();
        assert_eq!(v["DurableExecutions"].as_array().unwrap().len(), 1);
    }

    /// Seed a callback token as the service would when a task suspends.
    fn seed_callback(s: &SharedLambdaState, callback_id: &str) {
        let mut accts = s.write();
        let st = accts.regional_mut("123456789012", "us-east-1");
        st.durable_execution_callbacks.insert(
            callback_id.to_string(),
            DurableExecutionCallback {
                callback_id: callback_id.to_string(),
                execution_arn: "arn:aws:lambda:us-east-1:123456789012:durable-execution/e1"
                    .to_string(),
                outcome: "Pending".to_string(),
                recorded_at: Utc::now(),
            },
        );
    }

    #[test]
    fn send_callback_records_the_outcome_on_a_known_token() {
        let s = shared();
        for id in ["cb1", "cb2", "cb3"] {
            seed_callback(&s, id);
        }
        send_callback_success(&s, &req(), "cb1").unwrap();
        send_callback_failure(&s, &req(), "cb2").unwrap();
        send_callback_heartbeat(&s, &req(), "cb3").unwrap();
        let st = s.read();
        let cbs = &st
            .regional("123456789012", "us-east-1")
            .unwrap()
            .durable_execution_callbacks;
        assert_eq!(cbs["cb1"].outcome, "Succeeded");
        assert_eq!(cbs["cb2"].outcome, "Failed");
        assert_eq!(cbs["cb3"].outcome, "Heartbeat");
    }

    /// fakecloud has no suspending task to mint a token from, so an unknown
    /// callback id records the outcome against a synthesized parent execution
    /// rather than failing — these operations would otherwise never succeed.
    #[test]
    fn send_callback_records_an_unknown_token() {
        let s = shared();
        for (i, call) in [
            send_callback_success(&s, &req(), "bmV2ZXItaXNzdWVkLTE="),
            send_callback_failure(&s, &req(), "bmV2ZXItaXNzdWVkLTI="),
            send_callback_heartbeat(&s, &req(), "bmV2ZXItaXNzdWVkLTM="),
        ]
        .into_iter()
        .enumerate()
        {
            call.unwrap_or_else(|e| panic!("call {i} failed: {e:?}"));
        }
        let accts = s.read();
        let st = accts.regional("123456789012", "us-east-1").unwrap();
        for (id, outcome) in [
            ("bmV2ZXItaXNzdWVkLTE=", "Succeeded"),
            ("bmV2ZXItaXNzdWVkLTI=", "Failed"),
            ("bmV2ZXItaXNzdWVkLTM=", "Heartbeat"),
        ] {
            let cb = st
                .durable_execution_callbacks
                .get(id)
                .unwrap_or_else(|| panic!("{id} was not recorded"));
            assert_eq!(cb.outcome, outcome);
            assert!(!cb.execution_arn.is_empty());
        }
    }

    #[test]
    fn callback_id_must_match_the_base64_pattern() {
        for ok in ["cb1", "a+b/c", "YWJj", "YWI=", "YQ=="] {
            assert!(is_callback_id(ok), "{ok} should be accepted");
        }
        for bad in [
            "",
            "=",
            "a===",
            "a_b",
            "a-b",
            "a=b",
            "INVALID_OMITTED_LABEL",
        ] {
            assert!(!is_callback_id(bad), "{bad} should be rejected");
        }
    }

    #[test]
    fn send_callback_rejects_a_malformed_callback_id() {
        let s = shared();
        for call in [
            send_callback_success(&s, &req(), "bad_id"),
            send_callback_failure(&s, &req(), "bad-id"),
            send_callback_heartbeat(&s, &req(), "bad!id"),
        ] {
            let err = call
                .err()
                .expect("a malformed callback id must be rejected");
            assert_eq!(err.code(), "ValidationException");
        }
        let accts = s.read();
        assert!(accts
            .regional("123456789012", "us-east-1")
            .is_none_or(|st| st.durable_execution_callbacks.is_empty()));
    }

    #[test]
    fn listing_executions_of_an_unknown_function_is_not_found() {
        let s = shared();
        let err = list_durable_executions_by_function(&s, &req(), "never-created")
            .err()
            .expect("an unknown function must not list");
        assert_eq!(err.code(), "ResourceNotFoundException");
    }
}
