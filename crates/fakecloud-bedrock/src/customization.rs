use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::SharedBedrockState;

pub fn create_model_customization_job(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let job_name = body["jobName"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "jobName is required",
        )
    })?;

    let base_model = body["baseModelIdentifier"]
        .as_str()
        .unwrap_or("amazon.titan-text-express-v1");

    let custom_model_name = body["customModelName"].as_str().unwrap_or(job_name);

    let role_arn = body["roleArn"].as_str().unwrap_or_default();

    let job_id = Uuid::new_v4().to_string();
    let job_arn = format!(
        "arn:aws:bedrock:{}:{}:model-customization-job/{}",
        req.region, req.account_id, job_id
    );

    let now = Utc::now();
    let job = crate::state::CustomizationJob {
        job_arn: job_arn.clone(),
        job_name: job_name.to_string(),
        base_model_identifier: base_model.to_string(),
        custom_model_name: custom_model_name.to_string(),
        role_arn: role_arn.to_string(),
        training_data_config: body.get("trainingDataConfig").cloned().unwrap_or(json!({})),
        output_data_config: body.get("outputDataConfig").cloned().unwrap_or(json!({})),
        hyper_parameters: body
            .get("hyperParameters")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        status: "InProgress".to_string(),
        created_at: now,
        last_modified_at: now,
    };

    let mut s = state.write();
    s.customization_jobs.insert(job_arn.clone(), job);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "jobArn": job_arn })).unwrap(),
    ))
}

pub fn get_model_customization_job(
    state: &SharedBedrockState,
    req: &AwsRequest,
    job_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();

    // Job identifier can be an ARN or a job name
    let job = s
        .customization_jobs
        .get(job_identifier)
        .or_else(|| {
            s.customization_jobs
                .values()
                .find(|j| j.job_name == job_identifier)
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Model customization job {job_identifier} not found"),
            )
        })?;

    let output_model_arn = format!(
        "arn:aws:bedrock:{}:{}:custom-model/{}",
        req.region, req.account_id, job.custom_model_name
    );

    Ok(AwsResponse::ok_json(json!({
        "jobArn": job.job_arn,
        "jobName": job.job_name,
        "outputModelName": job.custom_model_name,
        "outputModelArn": output_model_arn,
        "roleArn": job.role_arn,
        "status": job.status,
        "creationTime": job.created_at.to_rfc3339(),
        "lastModifiedTime": job.last_modified_at.to_rfc3339(),
        "baseModelIdentifier": job.base_model_identifier,
        "trainingDataConfig": job.training_data_config,
        "outputDataConfig": job.output_data_config,
        "hyperParameters": job.hyper_parameters,
    })))
}

pub fn list_model_customization_jobs(
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
    let mut items: Vec<&crate::state::CustomizationJob> = s.customization_jobs.values().collect();
    items.sort_by(|a, b| a.job_arn.cmp(&b.job_arn));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|j| j.job_arn.as_str() > token.as_str())
            .unwrap_or(items.len())
    } else {
        0
    };

    let page: Vec<Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|j| {
            json!({
                "jobArn": j.job_arn,
                "jobName": j.job_name,
                "status": j.status,
                "baseModelIdentifier": j.base_model_identifier,
                "customModelName": j.custom_model_name,
                "creationTime": j.created_at.to_rfc3339(),
                "lastModifiedTime": j.last_modified_at.to_rfc3339(),
            })
        })
        .collect();

    let mut resp = json!({ "modelCustomizationJobSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.job_arn);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn stop_model_customization_job(
    state: &SharedBedrockState,
    job_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    // Find the key first to avoid double mutable borrow
    let key = s
        .customization_jobs
        .iter()
        .find(|(k, j)| *k == job_identifier || j.job_name == job_identifier)
        .map(|(k, _)| k.clone())
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Model customization job {job_identifier} not found"),
            )
        })?;

    let job = s.customization_jobs.get_mut(&key).unwrap();

    if job.status != "InProgress" {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "ConflictException",
            format!("Job is not in InProgress status (current: {})", job.status),
        ));
    }

    job.status = "Stopped".to_string();
    job.last_modified_at = Utc::now();

    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}
