use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{ProvisionedThroughput, SharedBedrockState};

pub fn create_provisioned_model_throughput(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let provisioned_model_name = body["provisionedModelName"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "provisionedModelName is required",
        )
    })?;

    let model_id = body["modelId"].as_str().unwrap_or_default();
    let model_units = body["modelUnits"].as_i64().unwrap_or(1) as i32;

    if model_units < 1 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "modelUnits must be at least 1",
        ));
    }

    if let Some(duration) = body["commitmentDuration"].as_str() {
        if !["OneMonth", "SixMonths"].contains(&duration) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "Invalid commitmentDuration: {duration}. Valid values: OneMonth, SixMonths"
                ),
            ));
        }
    }

    let provisioned_model_id = Uuid::new_v4().to_string()[..12].to_string();
    let provisioned_model_arn = format!(
        "arn:aws:bedrock:{}:{}:provisioned-model/{}",
        req.region, req.account_id, provisioned_model_id
    );

    let model_arn = if model_id.contains(':') {
        model_id.to_string()
    } else {
        format!(
            "arn:aws:bedrock:{}::foundation-model/{}",
            req.region, model_id
        )
    };

    let now = Utc::now();
    let throughput = ProvisionedThroughput {
        provisioned_model_id: provisioned_model_id.clone(),
        provisioned_model_arn: provisioned_model_arn.clone(),
        provisioned_model_name: provisioned_model_name.to_string(),
        model_arn,
        model_units,
        desired_model_units: model_units,
        status: "InService".to_string(),
        commitment_duration: body["commitmentDuration"].as_str().map(|s| s.to_string()),
        created_at: now,
        last_modified_at: now,
    };

    let mut s = state.write();
    s.provisioned_throughputs
        .insert(provisioned_model_id, throughput);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({
            "provisionedModelArn": provisioned_model_arn,
        }))
        .unwrap(),
    ))
}

pub fn get_provisioned_model_throughput(
    state: &SharedBedrockState,
    provisioned_model_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let throughput = find_throughput(&s.provisioned_throughputs, provisioned_model_id)?;

    Ok(AwsResponse::ok_json(throughput_to_json(throughput)))
}

pub fn list_provisioned_model_throughputs(
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
    let mut items: Vec<&ProvisionedThroughput> = s.provisioned_throughputs.values().collect();
    items.sort_by(|a, b| a.provisioned_model_id.cmp(&b.provisioned_model_id));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|t| t.provisioned_model_id.as_str() > token.as_str())
            .unwrap_or(items.len())
    } else {
        0
    };

    let page: Vec<Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|t| {
            json!({
                "provisionedModelName": t.provisioned_model_name,
                "provisionedModelArn": t.provisioned_model_arn,
                "modelArn": t.model_arn,
                "desiredModelArn": t.model_arn,
                "foundationModelArn": t.model_arn,
                "status": t.status,
                "modelUnits": t.model_units,
                "desiredModelUnits": t.desired_model_units,
                "creationTime": t.created_at.to_rfc3339(),
                "lastModifiedTime": t.last_modified_at.to_rfc3339(),
            })
        })
        .collect();

    let mut resp = json!({ "provisionedModelSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.provisioned_model_id);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn update_provisioned_model_throughput(
    state: &SharedBedrockState,
    provisioned_model_id: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
    let throughput = find_throughput_mut(&mut s.provisioned_throughputs, provisioned_model_id)?;

    if let Some(units) = body["desiredModelUnits"].as_i64() {
        throughput.desired_model_units = units as i32;
        throughput.model_units = units as i32;
    }
    if let Some(name) = body["desiredProvisionedModelName"].as_str() {
        throughput.provisioned_model_name = name.to_string();
    }
    throughput.last_modified_at = Utc::now();

    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}

pub fn delete_provisioned_model_throughput(
    state: &SharedBedrockState,
    provisioned_model_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    // Find by ID or ARN
    let key = s
        .provisioned_throughputs
        .iter()
        .find(|(_, t)| {
            t.provisioned_model_id == provisioned_model_id
                || t.provisioned_model_arn == provisioned_model_id
        })
        .map(|(k, _)| k.clone());

    match key {
        Some(k) => {
            s.provisioned_throughputs.remove(&k);
            Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
        }
        None => Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Provisioned model {provisioned_model_id} not found"),
        )),
    }
}

fn find_throughput<'a>(
    throughputs: &'a std::collections::HashMap<String, ProvisionedThroughput>,
    id_or_arn: &str,
) -> Result<&'a ProvisionedThroughput, AwsServiceError> {
    throughputs
        .get(id_or_arn)
        .or_else(|| {
            throughputs
                .values()
                .find(|t| t.provisioned_model_arn == id_or_arn)
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Provisioned model {id_or_arn} not found"),
            )
        })
}

fn find_throughput_mut<'a>(
    throughputs: &'a mut std::collections::HashMap<String, ProvisionedThroughput>,
    id_or_arn: &str,
) -> Result<&'a mut ProvisionedThroughput, AwsServiceError> {
    // First find the key
    let key = throughputs
        .iter()
        .find(|(k, t)| *k == id_or_arn || t.provisioned_model_arn == id_or_arn)
        .map(|(k, _)| k.clone())
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Provisioned model {id_or_arn} not found"),
            )
        })?;
    Ok(throughputs.get_mut(&key).unwrap())
}

fn throughput_to_json(t: &ProvisionedThroughput) -> Value {
    json!({
        "provisionedModelName": t.provisioned_model_name,
        "provisionedModelArn": t.provisioned_model_arn,
        "modelArn": t.model_arn,
        "desiredModelArn": t.model_arn,
        "foundationModelArn": t.model_arn,
        "status": t.status,
        "modelUnits": t.model_units,
        "desiredModelUnits": t.desired_model_units,
        "commitmentDuration": t.commitment_duration,
        "creationTime": t.created_at.to_rfc3339(),
        "lastModifiedTime": t.last_modified_at.to_rfc3339(),
    })
}
