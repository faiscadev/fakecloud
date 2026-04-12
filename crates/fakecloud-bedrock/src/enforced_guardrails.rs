use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::SharedBedrockState;

pub fn put_enforced_guardrail_configuration(
    state: &SharedBedrockState,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let config_id = Uuid::new_v4().to_string();

    let mut s = state.write();
    s.enforced_guardrail_configs
        .insert(config_id.clone(), body.clone());

    Ok(AwsResponse::ok_json(json!({
        "configId": config_id,
    })))
}

pub fn list_enforced_guardrails_configuration(
    state: &SharedBedrockState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let max_results = req
        .query_params
        .get("maxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100)
        .max(1);
    let next_token = req.query_params.get("nextToken");

    let s = state.read();
    let mut items: Vec<(&String, &Value)> = s.enforced_guardrail_configs.iter().collect();
    items.sort_by(|a, b| a.0.cmp(b.0));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|(k, _)| k.as_str() > token.as_str())
            .unwrap_or(items.len())
    } else {
        0
    };

    let page: Vec<Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|(k, v)| {
            let mut entry = (*v).clone();
            if let Some(obj) = entry.as_object_mut() {
                obj.insert("configId".to_string(), json!(k));
            }
            entry
        })
        .collect();

    let mut resp = json!({ "enforcedGuardrailsConfigurations": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.0);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn delete_enforced_guardrail_configuration(
    state: &SharedBedrockState,
    config_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    match s.enforced_guardrail_configs.remove(config_id) {
        Some(_) => Ok(AwsResponse::json(StatusCode::OK, "{}".to_string())),
        None => Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Enforced guardrail configuration {config_id} not found"),
        )),
    }
}
