use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{EvaluationJob, SharedBedrockState};

pub fn create_evaluation_job(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let job_name = body["jobName"].as_str().unwrap_or("eval-job");
    let role_arn = body["roleArn"].as_str().unwrap_or_default();

    let job_id = Uuid::new_v4().to_string();
    let job_arn = format!(
        "arn:aws:bedrock:{}:{}:evaluation-job/{}",
        req.region, req.account_id, job_id
    );

    let now = Utc::now();
    let job = EvaluationJob {
        job_arn: job_arn.clone(),
        job_name: job_name.to_string(),
        job_description: body["jobDescription"].as_str().map(|s| s.to_string()),
        role_arn: role_arn.to_string(),
        status: "InProgress".to_string(),
        job_type: body["evaluationConfig"]["automated"]
            .as_object()
            .map(|_| "Automated")
            .unwrap_or("Human")
            .to_string(),
        evaluation_config: body.get("evaluationConfig").cloned().unwrap_or(json!({})),
        inference_config: body.get("inferenceConfig").cloned().unwrap_or(json!({})),
        output_data_config: body.get("outputDataConfig").cloned().unwrap_or(json!({})),
        creation_time: now,
        last_modified_time: now,
    };

    let mut s = state.write();
    s.evaluation_jobs.insert(job_arn.clone(), job);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "jobArn": job_arn })).unwrap(),
    ))
}

pub fn get_evaluation_job(
    state: &SharedBedrockState,
    job_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let job = find_job(&s.evaluation_jobs, job_identifier)?;

    Ok(AwsResponse::ok_json(json!({
        "jobArn": job.job_arn,
        "jobName": job.job_name,
        "jobDescription": job.job_description,
        "roleArn": job.role_arn,
        "status": job.status,
        "jobType": job.job_type,
        "evaluationConfig": job.evaluation_config,
        "inferenceConfig": job.inference_config,
        "outputDataConfig": job.output_data_config,
        "creationTime": job.creation_time.to_rfc3339(),
        "lastModifiedTime": job.last_modified_time.to_rfc3339(),
    })))
}

pub fn list_evaluation_jobs(
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
    let mut items: Vec<&EvaluationJob> = s.evaluation_jobs.values().collect();
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
                "jobType": j.job_type,
                "creationTime": j.creation_time.to_rfc3339(),
            })
        })
        .collect();

    let mut resp = json!({ "jobSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.job_arn);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn stop_evaluation_job(
    state: &SharedBedrockState,
    job_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
    let key = find_job_key(&s.evaluation_jobs, job_identifier)?;
    let job = s.evaluation_jobs.get_mut(&key).unwrap();

    if job.status != "InProgress" {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "ConflictException",
            format!("Job is not in InProgress status (current: {})", job.status),
        ));
    }

    job.status = "Stopped".to_string();
    job.last_modified_time = Utc::now();

    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}

pub fn batch_delete_evaluation_job(
    state: &SharedBedrockState,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let job_identifiers = body["jobIdentifiers"].as_array().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "jobIdentifiers is required",
        )
    })?;

    let mut s = state.write();
    let mut errors: Vec<Value> = Vec::new();

    for identifier in job_identifiers {
        let id = identifier.as_str().unwrap_or_default();
        let key = s
            .evaluation_jobs
            .iter()
            .find(|(k, j)| *k == id || j.job_name == id || j.job_arn.ends_with(&format!("/{id}")))
            .map(|(k, _)| k.clone());

        match key {
            Some(k) => {
                s.evaluation_jobs.remove(&k);
            }
            None => {
                errors.push(json!({
                    "jobIdentifier": id,
                    "code": "JobNotFound",
                    "message": format!("Evaluation job {id} not found")
                }));
            }
        }
    }

    Ok(AwsResponse::ok_json(json!({ "errors": errors })))
}

fn find_job<'a>(
    jobs: &'a std::collections::HashMap<String, EvaluationJob>,
    id_or_arn: &str,
) -> Result<&'a EvaluationJob, AwsServiceError> {
    jobs.get(id_or_arn)
        .or_else(|| {
            jobs.values()
                .find(|j| j.job_name == id_or_arn || j.job_arn.ends_with(&format!("/{id_or_arn}")))
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Evaluation job {id_or_arn} not found"),
            )
        })
}

fn find_job_key(
    jobs: &std::collections::HashMap<String, EvaluationJob>,
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
                format!("Evaluation job {id_or_arn} not found"),
            )
        })
}
