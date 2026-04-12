use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{ModelInvocationJob, SharedBedrockState};

pub fn create_model_invocation_job(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let job_name = body["jobName"].as_str().unwrap_or("invocation-job");
    let model_id = body["modelId"].as_str().unwrap_or_default();
    let role_arn = body["roleArn"].as_str().unwrap_or_default();

    let job_id = Uuid::new_v4().to_string();
    let job_arn = format!(
        "arn:aws:bedrock:{}:{}:model-invocation-job/{}",
        req.region, req.account_id, job_id
    );

    let now = Utc::now();
    let job = ModelInvocationJob {
        job_arn: job_arn.clone(),
        job_name: job_name.to_string(),
        model_id: model_id.to_string(),
        role_arn: role_arn.to_string(),
        input_data_config: body.get("inputDataConfig").cloned().unwrap_or(json!({})),
        output_data_config: body.get("outputDataConfig").cloned().unwrap_or(json!({})),
        status: "InProgress".to_string(),
        submit_time: now,
        last_modified_time: now,
        end_time: None,
    };

    let mut s = state.write();
    s.model_invocation_jobs.insert(job_arn.clone(), job);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "jobArn": job_arn })).unwrap(),
    ))
}

pub fn get_model_invocation_job(
    state: &SharedBedrockState,
    job_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let job = find_job(&s.model_invocation_jobs, job_identifier)?;

    let mut resp = json!({
        "jobArn": job.job_arn,
        "jobName": job.job_name,
        "modelId": job.model_id,
        "roleArn": job.role_arn,
        "status": job.status,
        "inputDataConfig": job.input_data_config,
        "outputDataConfig": job.output_data_config,
        "submitTime": job.submit_time.to_rfc3339(),
        "lastModifiedTime": job.last_modified_time.to_rfc3339(),
    });
    if let Some(ref end_time) = job.end_time {
        resp["endTime"] = json!(end_time.to_rfc3339());
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn list_model_invocation_jobs(
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
    let mut items: Vec<&ModelInvocationJob> = s.model_invocation_jobs.values().collect();
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
                "modelId": j.model_id,
                "status": j.status,
                "submitTime": j.submit_time.to_rfc3339(),
                "lastModifiedTime": j.last_modified_time.to_rfc3339(),
            })
        })
        .collect();

    let mut resp = json!({ "invocationJobSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.job_arn);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn stop_model_invocation_job(
    state: &SharedBedrockState,
    job_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
    let key = find_job_key(&s.model_invocation_jobs, job_identifier)?;
    let job = s.model_invocation_jobs.get_mut(&key).unwrap();

    if job.status != "InProgress" {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "ConflictException",
            format!("Job is not in InProgress status (current: {})", job.status),
        ));
    }

    let now = Utc::now();
    job.status = "Stopped".to_string();
    job.last_modified_time = now;
    job.end_time = Some(now);

    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}

fn find_job<'a>(
    jobs: &'a std::collections::HashMap<String, ModelInvocationJob>,
    id_or_arn: &str,
) -> Result<&'a ModelInvocationJob, AwsServiceError> {
    jobs.get(id_or_arn)
        .or_else(|| {
            jobs.values()
                .find(|j| j.job_name == id_or_arn || j.job_arn.ends_with(&format!("/{id_or_arn}")))
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Model invocation job {id_or_arn} not found"),
            )
        })
}

fn find_job_key(
    jobs: &std::collections::HashMap<String, ModelInvocationJob>,
    id_or_arn: &str,
) -> Result<String, AwsServiceError> {
    jobs.iter()
        .find(|(k, j)| {
            *k == id_or_arn
                || j.job_name == id_or_arn
                || j.job_arn.ends_with(&format!("/{id_or_arn}"))
        })
        .map(|(k, _)| k.clone())
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Model invocation job {id_or_arn} not found"),
            )
        })
}
