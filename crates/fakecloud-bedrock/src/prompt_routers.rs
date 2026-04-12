use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{PromptRouter, SharedBedrockState};

pub fn create_prompt_router(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let default_name = Uuid::new_v4().to_string()[..8].to_string();
    let router_name = body["promptRouterName"].as_str().unwrap_or(&default_name);

    let router_id = Uuid::new_v4().to_string();
    let router_arn = format!(
        "arn:aws:bedrock:{}:{}:prompt-router/{}",
        req.region, req.account_id, router_id
    );

    let now = Utc::now();
    let router = PromptRouter {
        prompt_router_arn: router_arn.clone(),
        prompt_router_name: router_name.to_string(),
        description: body["description"].as_str().map(|s| s.to_string()),
        models: body.get("models").cloned().unwrap_or(json!([])),
        routing_criteria: body.get("routingCriteria").cloned().unwrap_or(json!({})),
        fallback_model: body.get("fallbackModel").cloned().unwrap_or(json!({})),
        status: "Active".to_string(),
        prompt_router_type: "custom".to_string(),
        created_at: now,
        updated_at: now,
    };

    let mut s = state.write();
    s.prompt_routers.insert(router_arn.clone(), router);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "promptRouterArn": router_arn })).unwrap(),
    ))
}

pub fn get_prompt_router(
    state: &SharedBedrockState,
    identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let router = s
        .prompt_routers
        .get(identifier)
        .or_else(|| {
            s.prompt_routers.values().find(|r| {
                r.prompt_router_name == identifier
                    || r.prompt_router_arn.ends_with(&format!("/{identifier}"))
            })
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Prompt router {identifier} not found"),
            )
        })?;

    Ok(AwsResponse::ok_json(json!({
        "promptRouterArn": router.prompt_router_arn,
        "promptRouterName": router.prompt_router_name,
        "description": router.description,
        "models": router.models,
        "routingCriteria": router.routing_criteria,
        "fallbackModel": router.fallback_model,
        "status": router.status,
        "type": router.prompt_router_type,
        "createdAt": router.created_at.to_rfc3339(),
        "updatedAt": router.updated_at.to_rfc3339(),
    })))
}

pub fn list_prompt_routers(
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
    let mut items: Vec<&PromptRouter> = s.prompt_routers.values().collect();
    items.sort_by(|a, b| a.prompt_router_arn.cmp(&b.prompt_router_arn));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|r| r.prompt_router_arn.as_str() > token.as_str())
            .unwrap_or(items.len())
    } else {
        0
    };

    let page: Vec<Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|r| {
            json!({
                "promptRouterArn": r.prompt_router_arn,
                "promptRouterName": r.prompt_router_name,
                "description": r.description,
                "status": r.status,
                "type": r.prompt_router_type,
                "createdAt": r.created_at.to_rfc3339(),
                "updatedAt": r.updated_at.to_rfc3339(),
            })
        })
        .collect();

    let mut resp = json!({ "promptRouterSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.prompt_router_arn);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn delete_prompt_router(
    state: &SharedBedrockState,
    identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let key = s
        .prompt_routers
        .iter()
        .find(|(k, r)| {
            *k == identifier
                || r.prompt_router_name == identifier
                || r.prompt_router_arn.ends_with(&format!("/{identifier}"))
        })
        .map(|(k, _)| k.clone());

    match key {
        Some(k) => {
            s.prompt_routers.remove(&k);
            Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
        }
        None => Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Prompt router {identifier} not found"),
        )),
    }
}
