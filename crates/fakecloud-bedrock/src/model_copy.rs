use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{ModelCopyJob, SharedBedrockState};

pub fn create_model_copy_job(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let source_model_arn = body["sourceModelArn"].as_str().unwrap_or_default();
    let target_model_name = body["targetModelName"].as_str().unwrap_or("copied-model");

    let job_id = Uuid::new_v4().to_string();
    let job_arn = format!(
        "arn:aws:bedrock:{}:{}:model-copy-job/{}",
        req.region, req.account_id, job_id
    );
    let target_model_arn = format!(
        "arn:aws:bedrock:{}:{}:custom-model/{}",
        req.region, req.account_id, target_model_name
    );

    let job = ModelCopyJob {
        job_arn: job_arn.clone(),
        source_model_arn: source_model_arn.to_string(),
        target_model_arn,
        target_model_name: target_model_name.to_string(),
        status: "Completed".to_string(),
        creation_time: Utc::now(),
    };

    let mut s = state.write();
    s.model_copy_jobs.insert(job_arn.clone(), job);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "jobArn": job_arn })).unwrap(),
    ))
}

pub fn get_model_copy_job(
    state: &SharedBedrockState,
    job_arn: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let job = s
        .model_copy_jobs
        .get(job_arn)
        .or_else(|| {
            s.model_copy_jobs
                .values()
                .find(|j| j.job_arn.ends_with(&format!("/{job_arn}")))
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Model copy job {job_arn} not found"),
            )
        })?;

    Ok(AwsResponse::ok_json(json!({
        "jobArn": job.job_arn,
        "status": job.status,
        "creationTime": job.creation_time.to_rfc3339(),
        "sourceModelArn": job.source_model_arn,
        "targetModelArn": job.target_model_arn,
        "targetModelName": job.target_model_name,
    })))
}

pub fn list_model_copy_jobs(
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
    let mut items: Vec<&ModelCopyJob> = s.model_copy_jobs.values().collect();
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
                "status": j.status,
                "creationTime": j.creation_time.to_rfc3339(),
                "sourceModelArn": j.source_model_arn,
                "targetModelArn": j.target_model_arn,
            })
        })
        .collect();

    let mut resp = json!({ "modelCopyJobSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.job_arn);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}
