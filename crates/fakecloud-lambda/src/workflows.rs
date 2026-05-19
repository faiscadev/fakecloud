//! Lambda Workflows: Capacity Providers (2025-11-30 API) + Durable
//! Executions (2025-12-01 API). Adds 15 new operations on top of the
//! classic Lambda surface, exposing the AWS preview that lets functions
//! run inside customer-managed compute pools and execute long-running
//! state machines with callback hooks.

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

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
    format!("arn:aws:lambda:{region}:{account}:capacity-provider/{name}")
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
    let name = body["CapacityProviderName"]
        .as_str()
        .ok_or_else(|| validation("CapacityProviderName is required"))?
        .to_string();
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

    let arn = arn_for_capacity_provider(&req.region, &req.account_id, &name);

    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
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
        arn: arn.clone(),
        state: "Active".to_string(),
        vpc_config: vpc,
        permissions_config: perms,
        instance_requirements: body.get("InstanceRequirements").cloned(),
        scaling_config: body.get("CapacityProviderScalingConfig").cloned(),
        kms_key_arn: body["KmsKeyArn"].as_str().map(String::from),
        tags,
        last_modified: Utc::now(),
        function_versions: Vec::new(),
    };
    s.capacity_providers.insert(name, cp.clone());

    Ok(AwsResponse::json_value(
        StatusCode::ACCEPTED,
        json!({ "CapacityProvider": capacity_provider_json(&cp) }),
    ))
}

pub(crate) fn get_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("CapacityProviderName", name, 1, 140)?;
    let accts = state.read();
    let empty = crate::state::LambdaState::new(&req.account_id, &req.region);
    let s = accts.get(&req.account_id).unwrap_or(&empty);
    let cp = s
        .capacity_providers
        .get(name)
        .ok_or_else(|| not_found(format!("Capacity provider {name} not found")))?;

    Ok(AwsResponse::ok_json(
        json!({ "CapacityProvider": capacity_provider_json(cp) }),
    ))
}

pub(crate) fn list_capacity_providers(
    state: &SharedLambdaState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accts = state.read();
    let empty = crate::state::LambdaState::new(&req.account_id, &req.region);
    let s = accts.get(&req.account_id).unwrap_or(&empty);
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
    check_len("CapacityProviderName", name, 1, 140)?;
    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    let cp = s
        .capacity_providers
        .get_mut(name)
        .ok_or_else(|| not_found(format!("Capacity provider {name} not found")))?;
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
    let json = capacity_provider_json(cp);

    Ok(AwsResponse::json_value(
        StatusCode::ACCEPTED,
        json!({ "CapacityProvider": json }),
    ))
}

pub(crate) fn delete_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("CapacityProviderName", name, 1, 140)?;
    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    s.capacity_providers
        .remove(name)
        .ok_or_else(|| not_found(format!("Capacity provider {name} not found")))?;
    Ok(AwsResponse::json(StatusCode::ACCEPTED, "{}".to_string()))
}

pub(crate) fn list_function_versions_by_capacity_provider(
    state: &SharedLambdaState,
    req: &AwsRequest,
    name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("CapacityProviderName", name, 1, 140)?;
    let accts = state.read();
    let empty = crate::state::LambdaState::new(&req.account_id, &req.region);
    let s = accts.get(&req.account_id).unwrap_or(&empty);
    let cp = s
        .capacity_providers
        .get(name)
        .ok_or_else(|| not_found(format!("Capacity provider {name} not found")))?;

    Ok(AwsResponse::ok_json(json!({
        "FunctionVersions": cp.function_versions,
    })))
}

// ─── Durable executions ───────────────────────────────────────────────────

fn execution_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:lambda:{region}:{account}:durable-execution/{id}")
}

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
    let accts = state.read();
    let empty = crate::state::LambdaState::new(&req.account_id, &req.region);
    let s = accts.get(&req.account_id).unwrap_or(&empty);
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
    let s = accts.get(&req.account_id).unwrap_or(&empty);
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
    let s = accts.get(&req.account_id).unwrap_or(&empty);
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
    let s = accts.get(&req.account_id).unwrap_or(&empty);
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
    let s = accts.get_or_create(&req.account_id);
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
    let s = accts.get_or_create(&req.account_id);
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

fn record_callback(
    state: &SharedLambdaState,
    req: &AwsRequest,
    callback_id: &str,
    outcome: &str,
) -> Result<AwsResponse, AwsServiceError> {
    check_len("CallbackId", callback_id, 1, 1024)?;
    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    let existing_execution = s
        .durable_execution_callbacks
        .get(callback_id)
        .map(|cb| cb.execution_arn.clone());
    let execution_arn = existing_execution.unwrap_or_else(|| {
        // Synthesize a parent execution arn when the callback id is
        // unknown — callbacks created externally (out-of-band) still
        // need to be recordable so the workflow can resume.
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
        let prev_mod = s.read().default_ref().capacity_providers["cp1"].last_modified;
        std::thread::sleep(std::time::Duration::from_millis(2));
        update_capacity_provider(
            &s,
            &req(),
            "cp1",
            &json!({"PermissionsConfig": {"RoleArn": "new"}}),
        )
        .unwrap();
        let state = s.read();
        let cp = &state.default_ref().capacity_providers["cp1"];
        assert_eq!(cp.permissions_config["RoleArn"], "new");
        assert!(cp.last_modified > prev_mod);
    }

    fn seed_execution(s: &SharedLambdaState, arn: &str, function_name: &str, status: &str) {
        let mut accts = s.write();
        let st = accts.get_or_create("123456789012");
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
        let exec = s.read().default_ref().durable_executions[arn].clone();
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

    #[test]
    fn send_callback_records_outcome_and_creates_callback_if_unknown() {
        let s = shared();
        send_callback_success(&s, &req(), "cb1").unwrap();
        send_callback_failure(&s, &req(), "cb2").unwrap();
        send_callback_heartbeat(&s, &req(), "cb3").unwrap();
        let st = s.read();
        let cbs = &st.default_ref().durable_execution_callbacks;
        assert_eq!(cbs["cb1"].outcome, "Succeeded");
        assert_eq!(cbs["cb2"].outcome, "Failed");
        assert_eq!(cbs["cb3"].outcome, "Heartbeat");
    }
}
