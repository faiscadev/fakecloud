use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{CustomModelDeployment, SharedBedrockState};

pub fn create_custom_model_deployment(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let deployment_name = body["modelDeploymentName"].as_str().unwrap_or("deployment");
    let model_arn = body["modelArn"].as_str().unwrap_or_default();

    let deployment_id = Uuid::new_v4().to_string();
    let deployment_arn = format!(
        "arn:aws:bedrock:{}:{}:custom-model-deployment/{}",
        req.region, req.account_id, deployment_id
    );

    let now = Utc::now();
    let deployment = CustomModelDeployment {
        deployment_arn: deployment_arn.clone(),
        deployment_name: deployment_name.to_string(),
        model_arn: model_arn.to_string(),
        description: body["description"].as_str().map(|s| s.to_string()),
        status: "Active".to_string(),
        created_at: now,
        last_updated_at: now,
    };

    let mut s = state.write();
    s.custom_model_deployments
        .insert(deployment_arn.clone(), deployment);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "customModelDeploymentArn": deployment_arn })).unwrap(),
    ))
}

pub fn get_custom_model_deployment(
    state: &SharedBedrockState,
    deployment_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let deployment = find_deployment(&s.custom_model_deployments, deployment_identifier)?;

    Ok(AwsResponse::ok_json(deployment_to_json(deployment)))
}

pub fn list_custom_model_deployments(
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
    let mut items: Vec<&CustomModelDeployment> = s.custom_model_deployments.values().collect();
    items.sort_by(|a, b| a.deployment_arn.cmp(&b.deployment_arn));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|d| d.deployment_arn.as_str() > token.as_str())
            .unwrap_or(items.len())
    } else {
        0
    };

    let page: Vec<Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|d| deployment_summary_json(d))
        .collect();

    let mut resp = json!({ "modelDeploymentSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.deployment_arn);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn update_custom_model_deployment(
    state: &SharedBedrockState,
    deployment_identifier: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
    let key = find_deployment_key(&s.custom_model_deployments, deployment_identifier)?;
    let deployment = s.custom_model_deployments.get_mut(&key).unwrap();

    if let Some(model_arn) = body["modelArn"].as_str() {
        deployment.model_arn = model_arn.to_string();
    }
    deployment.last_updated_at = Utc::now();

    let arn = deployment.deployment_arn.clone();
    Ok(AwsResponse::ok_json(
        json!({ "customModelDeploymentArn": arn }),
    ))
}

pub fn delete_custom_model_deployment(
    state: &SharedBedrockState,
    deployment_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
    let key = find_deployment_key(&s.custom_model_deployments, deployment_identifier)?;
    s.custom_model_deployments.remove(&key);
    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}

fn find_deployment<'a>(
    deployments: &'a std::collections::HashMap<String, CustomModelDeployment>,
    id_or_arn: &str,
) -> Result<&'a CustomModelDeployment, AwsServiceError> {
    deployments
        .get(id_or_arn)
        .or_else(|| {
            deployments.values().find(|d| {
                d.deployment_name == id_or_arn
                    || d.deployment_arn.ends_with(&format!("/{id_or_arn}"))
            })
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Custom model deployment {id_or_arn} not found"),
            )
        })
}

fn find_deployment_key(
    deployments: &std::collections::HashMap<String, CustomModelDeployment>,
    id_or_arn: &str,
) -> Result<String, AwsServiceError> {
    deployments
        .iter()
        .find(|(k, d)| {
            *k == id_or_arn
                || d.deployment_name == id_or_arn
                || d.deployment_arn.ends_with(&format!("/{id_or_arn}"))
        })
        .map(|(k, _)| k.clone())
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Custom model deployment {id_or_arn} not found"),
            )
        })
}

fn deployment_to_json(d: &CustomModelDeployment) -> Value {
    let mut obj = json!({
        "customModelDeploymentArn": d.deployment_arn,
        "modelDeploymentName": d.deployment_name,
        "modelArn": d.model_arn,
        "status": d.status,
        "createdAt": d.created_at.to_rfc3339(),
        "lastUpdatedAt": d.last_updated_at.to_rfc3339(),
    });
    if let Some(ref desc) = d.description {
        obj["description"] = json!(desc);
    }
    obj
}

fn deployment_summary_json(d: &CustomModelDeployment) -> Value {
    let mut obj = json!({
        "customModelDeploymentArn": d.deployment_arn,
        "modelDeploymentName": d.deployment_name,
        "modelArn": d.model_arn,
        "status": d.status,
        "createdAt": d.created_at.to_rfc3339(),
        "lastUpdatedAt": d.last_updated_at.to_rfc3339(),
    });
    if let Some(ref desc) = d.description {
        obj["description"] = json!(desc);
    }
    obj
}
