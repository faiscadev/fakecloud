use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{CustomModel, SharedBedrockState};

pub fn create_custom_model(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let default_name = Uuid::new_v4().to_string()[..8].to_string();
    let model_name = body["modelName"].as_str().unwrap_or(&default_name);

    let model_id = Uuid::new_v4().to_string();
    let model_arn = format!(
        "arn:aws:bedrock:{}:{}:custom-model/{}",
        req.region, req.account_id, model_id
    );

    let model = CustomModel {
        model_arn: model_arn.clone(),
        model_name: model_name.to_string(),
        model_source_config: body.get("modelSourceConfig").cloned().unwrap_or(json!({})),
        model_kms_key_arn: body["modelKmsKeyArn"].as_str().map(|s| s.to_string()),
        role_arn: body["roleArn"].as_str().map(|s| s.to_string()),
        model_status: "Active".to_string(),
        creation_time: Utc::now(),
    };

    let mut s = state.write();
    s.custom_models.insert(model_arn.clone(), model);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "modelArn": model_arn })).unwrap(),
    ))
}

pub fn get_custom_model(
    state: &SharedBedrockState,
    model_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let model = s
        .custom_models
        .get(model_identifier)
        .or_else(|| {
            s.custom_models.values().find(|m| {
                m.model_name == model_identifier
                    || m.model_arn.ends_with(&format!("/{model_identifier}"))
            })
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Custom model {model_identifier} not found"),
            )
        })?;

    Ok(AwsResponse::ok_json(json!({
        "modelArn": model.model_arn,
        "modelName": model.model_name,
        "modelStatus": model.model_status,
        "creationTime": model.creation_time.to_rfc3339(),
    })))
}

pub fn list_custom_models(
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
    let mut items: Vec<&CustomModel> = s.custom_models.values().collect();
    items.sort_by(|a, b| a.model_arn.cmp(&b.model_arn));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|m| m.model_arn.as_str() > token.as_str())
            .unwrap_or(items.len())
    } else {
        0
    };

    let page: Vec<Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|m| {
            json!({
                "modelArn": m.model_arn,
                "modelName": m.model_name,
                "modelStatus": m.model_status,
                "creationTime": m.creation_time.to_rfc3339(),
            })
        })
        .collect();

    let mut resp = json!({ "modelSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.model_arn);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn delete_custom_model(
    state: &SharedBedrockState,
    model_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let key = s
        .custom_models
        .iter()
        .find(|(k, m)| {
            *k == model_identifier
                || m.model_name == model_identifier
                || m.model_arn.ends_with(&format!("/{model_identifier}"))
        })
        .map(|(k, _)| k.clone());

    match key {
        Some(k) => {
            s.custom_models.remove(&k);
            Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
        }
        None => Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Custom model {model_identifier} not found"),
        )),
    }
}
