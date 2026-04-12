use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{AsyncInvocation, SharedBedrockState};

pub fn start_async_invoke(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let model_id = body["modelId"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "modelId is required",
        )
    })?;

    let output_data_config = body.get("outputDataConfig").ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "outputDataConfig is required",
        )
    })?;

    let model_input = body.get("modelInput").ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "modelInput is required",
        )
    })?;

    let invocation_id = Uuid::new_v4().to_string();
    let invocation_arn = format!(
        "arn:aws:bedrock:{}:{}:async-invoke/{}",
        req.region, req.account_id, invocation_id
    );

    let model_arn = if model_id.starts_with("arn:") {
        model_id.to_string()
    } else {
        format!(
            "arn:aws:bedrock:{}::foundation-model/{}",
            req.region, model_id
        )
    };

    let now = Utc::now();
    let invocation = AsyncInvocation {
        invocation_arn: invocation_arn.clone(),
        model_arn,
        model_input: model_input.clone(),
        output_data_config: output_data_config.clone(),
        client_request_token: body["clientRequestToken"].as_str().map(|s| s.to_string()),
        status: "Completed".to_string(),
        submit_time: now,
        last_modified_time: now,
        end_time: Some(now),
    };

    let mut s = state.write();
    s.async_invocations
        .insert(invocation_arn.clone(), invocation);

    Ok(AwsResponse::json(
        StatusCode::OK,
        serde_json::to_string(&json!({ "invocationArn": invocation_arn })).unwrap(),
    ))
}

pub fn get_async_invoke(
    state: &SharedBedrockState,
    invocation_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    // Look up by full ARN or by the UUID suffix
    let invocation = s
        .async_invocations
        .get(invocation_id)
        .or_else(|| {
            s.async_invocations
                .values()
                .find(|inv| inv.invocation_arn.ends_with(invocation_id))
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Async invocation {invocation_id} not found"),
            )
        })?;

    Ok(AwsResponse::ok_json(invocation_to_json(invocation)))
}

pub fn list_async_invokes(
    state: &SharedBedrockState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let max_results = req
        .query_params
        .get("maxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100);
    let next_token = req.query_params.get("nextToken");
    let status_filter = req.query_params.get("statusEquals");

    let s = state.read();
    let mut items: Vec<&AsyncInvocation> = s
        .async_invocations
        .values()
        .filter(|inv| {
            if let Some(status) = status_filter {
                inv.status == *status
            } else {
                true
            }
        })
        .collect();
    items.sort_by(|a, b| b.submit_time.cmp(&a.submit_time)); // Descending by default

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|inv| inv.invocation_arn.as_str() == token.as_str())
            .map(|p| p + 1)
            .unwrap_or(0)
    } else {
        0
    };

    let page: Vec<Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|inv| invocation_summary_json(inv))
        .collect();

    let mut resp = json!({ "asyncInvokeSummaries": page });
    if start + max_results < items.len() {
        if let Some(last) = items.get(start + max_results - 1) {
            resp["nextToken"] = json!(last.invocation_arn);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

fn invocation_to_json(inv: &AsyncInvocation) -> Value {
    let mut obj = json!({
        "invocationArn": inv.invocation_arn,
        "modelArn": inv.model_arn,
        "status": inv.status,
        "submitTime": inv.submit_time.to_rfc3339(),
        "lastModifiedTime": inv.last_modified_time.to_rfc3339(),
        "outputDataConfig": inv.output_data_config,
    });
    if let Some(ref token) = inv.client_request_token {
        obj["clientRequestToken"] = json!(token);
    }
    if let Some(ref end_time) = inv.end_time {
        obj["endTime"] = json!(end_time.to_rfc3339());
    }
    obj
}

fn invocation_summary_json(inv: &AsyncInvocation) -> Value {
    let mut obj = json!({
        "invocationArn": inv.invocation_arn,
        "modelArn": inv.model_arn,
        "status": inv.status,
        "submitTime": inv.submit_time.to_rfc3339(),
        "lastModifiedTime": inv.last_modified_time.to_rfc3339(),
        "outputDataConfig": inv.output_data_config,
    });
    if let Some(ref end_time) = inv.end_time {
        obj["endTime"] = json!(end_time.to_rfc3339());
    }
    obj
}
