use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{MarketplaceModelEndpoint, SharedBedrockState};

pub fn create_marketplace_model_endpoint(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let endpoint_name = body["endpointName"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "endpointName is required",
        )
    })?;

    let model_source_identifier = body["modelSourceIdentifier"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "modelSourceIdentifier is required",
        )
    })?;

    let endpoint_id = Uuid::new_v4().to_string();
    let endpoint_arn = format!(
        "arn:aws:bedrock:{}:{}:marketplace-model-endpoint/{}",
        req.region, req.account_id, endpoint_id
    );

    let now = Utc::now();
    let endpoint = MarketplaceModelEndpoint {
        endpoint_arn: endpoint_arn.clone(),
        endpoint_name: endpoint_name.to_string(),
        model_source_identifier: model_source_identifier.to_string(),
        status: "Active".to_string(),
        endpoint_config: body.get("endpointConfig").cloned().unwrap_or(json!({})),
        created_at: now,
        updated_at: now,
    };

    let mut s = state.write();
    s.marketplace_endpoints
        .insert(endpoint_arn.clone(), endpoint);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "marketplaceModelEndpointArn": endpoint_arn })).unwrap(),
    ))
}

pub fn get_marketplace_model_endpoint(
    state: &SharedBedrockState,
    identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let endpoint = s
        .marketplace_endpoints
        .get(identifier)
        .or_else(|| {
            s.marketplace_endpoints.values().find(|e| {
                e.endpoint_name == identifier || e.endpoint_arn.ends_with(&format!("/{identifier}"))
            })
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Marketplace model endpoint {identifier} not found"),
            )
        })?;

    Ok(AwsResponse::ok_json(json!({
        "marketplaceModelEndpoint": {
            "endpointArn": endpoint.endpoint_arn,
            "endpointName": endpoint.endpoint_name,
            "modelSourceIdentifier": endpoint.model_source_identifier,
            "status": endpoint.status,
            "endpointConfig": endpoint.endpoint_config,
            "createdAt": endpoint.created_at.to_rfc3339(),
            "updatedAt": endpoint.updated_at.to_rfc3339(),
        }
    })))
}

pub fn list_marketplace_model_endpoints(
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
    let mut items: Vec<&MarketplaceModelEndpoint> = s.marketplace_endpoints.values().collect();
    items.sort_by(|a, b| a.endpoint_arn.cmp(&b.endpoint_arn));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|e| e.endpoint_arn.as_str() > token.as_str())
            .unwrap_or(items.len())
    } else {
        0
    };

    let page: Vec<Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|e| {
            json!({
                "endpointArn": e.endpoint_arn,
                "endpointName": e.endpoint_name,
                "modelSourceIdentifier": e.model_source_identifier,
                "status": e.status,
                "createdAt": e.created_at.to_rfc3339(),
                "updatedAt": e.updated_at.to_rfc3339(),
            })
        })
        .collect();

    let mut resp = json!({ "marketplaceModelEndpoints": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.endpoint_arn);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn update_marketplace_model_endpoint(
    state: &SharedBedrockState,
    identifier: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let key = s
        .marketplace_endpoints
        .iter()
        .find(|(k, e)| {
            *k == identifier
                || e.endpoint_name == identifier
                || e.endpoint_arn.ends_with(&format!("/{identifier}"))
        })
        .map(|(k, _)| k.clone())
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Marketplace model endpoint {identifier} not found"),
            )
        })?;

    let endpoint = s.marketplace_endpoints.get_mut(&key).unwrap();

    if let Some(config) = body.get("endpointConfig") {
        endpoint.endpoint_config = config.clone();
    }
    endpoint.updated_at = Utc::now();

    Ok(AwsResponse::ok_json(json!({
        "marketplaceModelEndpoint": {
            "endpointArn": endpoint.endpoint_arn,
            "endpointName": endpoint.endpoint_name,
            "modelSourceIdentifier": endpoint.model_source_identifier,
            "status": endpoint.status,
            "endpointConfig": endpoint.endpoint_config,
            "createdAt": endpoint.created_at.to_rfc3339(),
            "updatedAt": endpoint.updated_at.to_rfc3339(),
        }
    })))
}

pub fn delete_marketplace_model_endpoint(
    state: &SharedBedrockState,
    identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let key = s
        .marketplace_endpoints
        .iter()
        .find(|(k, e)| {
            *k == identifier
                || e.endpoint_name == identifier
                || e.endpoint_arn.ends_with(&format!("/{identifier}"))
        })
        .map(|(k, _)| k.clone());

    match key {
        Some(k) => {
            s.marketplace_endpoints.remove(&k);
            Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
        }
        None => Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Marketplace model endpoint {identifier} not found"),
        )),
    }
}

pub fn register_marketplace_model_endpoint(
    state: &SharedBedrockState,
    identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let key = s
        .marketplace_endpoints
        .iter()
        .find(|(k, e)| {
            *k == identifier
                || e.endpoint_name == identifier
                || e.endpoint_arn.ends_with(&format!("/{identifier}"))
        })
        .map(|(k, _)| k.clone())
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Marketplace model endpoint {identifier} not found"),
            )
        })?;

    let endpoint = s.marketplace_endpoints.get_mut(&key).unwrap();
    endpoint.status = "Registered".to_string();
    endpoint.updated_at = Utc::now();

    Ok(AwsResponse::ok_json(json!({
        "marketplaceModelEndpoint": {
            "endpointArn": endpoint.endpoint_arn,
            "endpointName": endpoint.endpoint_name,
            "modelSourceIdentifier": endpoint.model_source_identifier,
            "status": endpoint.status,
            "endpointConfig": endpoint.endpoint_config,
            "createdAt": endpoint.created_at.to_rfc3339(),
            "updatedAt": endpoint.updated_at.to_rfc3339(),
        }
    })))
}

pub fn deregister_marketplace_model_endpoint(
    state: &SharedBedrockState,
    identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let key = s
        .marketplace_endpoints
        .iter()
        .find(|(k, e)| {
            *k == identifier
                || e.endpoint_name == identifier
                || e.endpoint_arn.ends_with(&format!("/{identifier}"))
        })
        .map(|(k, _)| k.clone())
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Marketplace model endpoint {identifier} not found"),
            )
        })?;

    let endpoint = s.marketplace_endpoints.get_mut(&key).unwrap();
    endpoint.status = "Active".to_string();
    endpoint.updated_at = Utc::now();

    Ok(AwsResponse::ok_json(json!({
        "marketplaceModelEndpoint": {
            "endpointArn": endpoint.endpoint_arn,
            "endpointName": endpoint.endpoint_name,
            "modelSourceIdentifier": endpoint.model_source_identifier,
            "status": endpoint.status,
            "endpointConfig": endpoint.endpoint_config,
            "createdAt": endpoint.created_at.to_rfc3339(),
            "updatedAt": endpoint.updated_at.to_rfc3339(),
        }
    })))
}
