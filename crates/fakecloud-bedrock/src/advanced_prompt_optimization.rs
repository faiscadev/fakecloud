use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{AdvancedPromptOptimizationJob, SharedBedrockState};

fn validation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

fn check_str_len(field: &str, val: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if val.len() < min || val.len() > max {
        return Err(validation(format!(
            "{field} length must be in [{min},{max}], got {}",
            val.len()
        )));
    }
    Ok(())
}

fn check_optional_str_len(
    field: &str,
    v: Option<&str>,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    if let Some(s) = v {
        check_str_len(field, s, min, max)?;
    }
    Ok(())
}

fn check_optional_enum(
    field: &str,
    v: Option<&str>,
    allowed: &[&str],
) -> Result<(), AwsServiceError> {
    if let Some(s) = v {
        if !allowed.contains(&s) {
            return Err(validation(format!(
                "{field} must be one of {allowed:?}, got '{s}'"
            )));
        }
    }
    Ok(())
}

pub(crate) fn create_advanced_prompt_optimization_job(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let job_name = body["jobName"]
        .as_str()
        .ok_or_else(|| validation("jobName is required"))?;
    check_str_len("jobName", job_name, 1, 100)?;
    check_optional_str_len("jobDescription", body["jobDescription"].as_str(), 1, 500)?;
    check_optional_str_len("clientToken", body["clientToken"].as_str(), 1, 256)?;
    check_optional_str_len(
        "encryptionKeyArn",
        body["encryptionKeyArn"].as_str(),
        1,
        2048,
    )?;
    if body.get("inputConfig").map(|v| v.is_null()).unwrap_or(true) {
        return Err(validation("inputConfig is required"));
    }
    if body
        .get("outputConfig")
        .map(|v| v.is_null())
        .unwrap_or(true)
    {
        return Err(validation("outputConfig is required"));
    }
    if body
        .get("modelConfigurations")
        .map(|v| v.is_null())
        .unwrap_or(true)
    {
        return Err(validation("modelConfigurations is required"));
    }

    let job_id = Uuid::new_v4().to_string();
    let job_arn = format!(
        "arn:aws:bedrock:{}:{}:advanced-prompt-optimization-job/{}",
        req.region, req.account_id, job_id
    );

    let now = Utc::now();
    let job = AdvancedPromptOptimizationJob {
        job_arn: job_arn.clone(),
        job_name: job_name.to_string(),
        job_description: body["jobDescription"].as_str().map(String::from),
        status: "InProgress".to_string(),
        input_config: body.get("inputConfig").cloned().unwrap_or(json!({})),
        output_config: body.get("outputConfig").cloned().unwrap_or(json!({})),
        encryption_key_arn: body["encryptionKeyArn"].as_str().map(String::from),
        model_configurations: body
            .get("modelConfigurations")
            .cloned()
            .unwrap_or(json!({})),
        creation_time: now,
        last_modified_time: now,
    };

    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    s.advanced_prompt_optimization_jobs
        .insert(job_arn.clone(), job);

    Ok(AwsResponse::json_value(
        StatusCode::CREATED,
        json!({ "jobArn": job_arn }),
    ))
}

pub(crate) fn get_advanced_prompt_optimization_job(
    state: &SharedBedrockState,
    req: &AwsRequest,
    job_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let accts = state.read();
    let empty = crate::state::BedrockState::new(&req.account_id, &req.region);
    let s = accts.get(&req.account_id).unwrap_or(&empty);
    let job = find_job(&s.advanced_prompt_optimization_jobs, job_identifier)?;

    Ok(AwsResponse::ok_json(json!({
        "jobArn": job.job_arn,
        "jobName": job.job_name,
        "jobDescription": job.job_description,
        "jobStatus": job.status,
        "inputConfig": job.input_config,
        "outputConfig": job.output_config,
        "encryptionKeyArn": job.encryption_key_arn,
        "modelConfigurations": job.model_configurations,
        "creationTime": job.creation_time.to_rfc3339(),
        "lastModifiedTime": job.last_modified_time.to_rfc3339(),
    })))
}

pub(crate) fn list_advanced_prompt_optimization_jobs(
    state: &SharedBedrockState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    if let Some(s) = req.query_params.get("maxResults") {
        let n: i64 = s
            .parse()
            .map_err(|_| validation("maxResults must be int"))?;
        if !(1..=1000).contains(&n) {
            return Err(validation(format!(
                "maxResults must be in [1,1000], got {n}"
            )));
        }
    }
    check_optional_str_len(
        "nextToken",
        req.query_params.get("nextToken").map(|s| s.as_str()),
        1,
        2048,
    )?;
    check_optional_enum(
        "sortBy",
        req.query_params.get("sortBy").map(|s| s.as_str()),
        &["CreationTime"],
    )?;
    check_optional_enum(
        "sortOrder",
        req.query_params.get("sortOrder").map(|s| s.as_str()),
        &["Ascending", "Descending"],
    )?;

    let max_results = req
        .query_params
        .get("maxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100)
        .max(1);
    let next_token = req.query_params.get("nextToken");

    let accts = state.read();
    let empty = crate::state::BedrockState::new(&req.account_id, &req.region);
    let s = accts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<&AdvancedPromptOptimizationJob> =
        s.advanced_prompt_optimization_jobs.values().collect();
    let sort_by = req
        .query_params
        .get("sortBy")
        .map(|s| s.as_str())
        .unwrap_or("CreationTime");
    let descending = matches!(
        req.query_params.get("sortOrder").map(|s| s.as_str()),
        Some("Descending")
    );
    items.sort_by(|a, b| {
        let ord = match sort_by {
            "CreationTime" => a.creation_time.cmp(&b.creation_time),
            _ => a.job_arn.cmp(&b.job_arn),
        };
        if descending {
            ord.reverse()
        } else {
            ord
        }
    });

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
                "jobStatus": j.status,
                "creationTime": j.creation_time.to_rfc3339(),
                "lastModifiedTime": j.last_modified_time.to_rfc3339(),
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

pub(crate) fn stop_advanced_prompt_optimization_job(
    state: &SharedBedrockState,
    req: &AwsRequest,
    job_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    let key = find_job_key(&s.advanced_prompt_optimization_jobs, job_identifier)?;
    let job = s
        .advanced_prompt_optimization_jobs
        .get_mut(&key)
        .expect("key validated");

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

pub(crate) fn batch_delete_advanced_prompt_optimization_job(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let job_identifiers = body["jobIdentifiers"].as_array().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "jobIdentifiers is required",
        )
    })?;

    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    let mut errors: Vec<Value> = Vec::new();
    let mut deleted: Vec<Value> = Vec::new();

    for identifier in job_identifiers {
        let id = identifier.as_str().unwrap_or_default();
        let key = s
            .advanced_prompt_optimization_jobs
            .iter()
            .find(|(k, j)| *k == id || j.job_name == id || j.job_arn.ends_with(&format!("/{id}")))
            .map(|(k, _)| k.clone());

        match key {
            Some(k) => {
                let job_arn = s
                    .advanced_prompt_optimization_jobs
                    .get(&k)
                    .map(|j| j.job_arn.clone())
                    .unwrap_or_else(|| k.clone());
                s.advanced_prompt_optimization_jobs.remove(&k);
                deleted.push(json!({
                    "jobIdentifier": job_arn,
                    "jobStatus": "Deleting",
                }));
            }
            None => {
                errors.push(json!({
                    "jobIdentifier": id,
                    "code": "JobNotFound",
                    "message": format!("Advanced prompt optimization job {id} not found")
                }));
            }
        }
    }

    Ok(AwsResponse::ok_json(json!({
        "errors": errors,
        "advancedPromptOptimizationJobs": deleted,
    })))
}

fn find_job<'a>(
    jobs: &'a std::collections::BTreeMap<String, AdvancedPromptOptimizationJob>,
    id_or_arn: &str,
) -> Result<&'a AdvancedPromptOptimizationJob, AwsServiceError> {
    jobs.get(id_or_arn)
        .or_else(|| {
            jobs.values()
                .find(|j| j.job_name == id_or_arn || j.job_arn.ends_with(&format!("/{id_or_arn}")))
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Advanced prompt optimization job {id_or_arn} not found"),
            )
        })
}

fn find_job_key(
    jobs: &std::collections::BTreeMap<String, AdvancedPromptOptimizationJob>,
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
                format!("Advanced prompt optimization job {id_or_arn} not found"),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn full_body(name: &str) -> Value {
        json!({
            "jobName": name,
            "inputConfig": {"s3Uri": "s3://b/in"},
            "outputConfig": {"s3Uri": "s3://b/out"},
            "modelConfigurations": {"targetModel": "anthropic.claude-3-sonnet"}
        })
    }

    fn shared() -> SharedBedrockState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                "123456789012",
                "us-east-1",
                "http://localhost:4566",
            ),
        ))
    }

    fn req() -> AwsRequest {
        AwsRequest {
            service: "bedrock".to_string(),
            action: "APO".to_string(),
            method: Method::POST,
            raw_path: "/".to_string(),
            raw_query: String::new(),
            path_segments: vec![],
            query_params: HashMap::new(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "req".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn create_returns_job_arn_and_stores_state() {
        let s = shared();
        let body = json!({
            "jobName": "opt-job-1",
            "inputConfig": {"s3Uri": "s3://b/in"},
            "outputConfig": {"s3Uri": "s3://b/out"},
            "modelConfigurations": {"targetModel": "anthropic.claude-3-sonnet"}
        });
        let resp = create_advanced_prompt_optimization_job(&s, &req(), &body).unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);
        assert_eq!(
            s.read()
                .default_ref()
                .advanced_prompt_optimization_jobs
                .len(),
            1
        );
    }

    #[test]
    fn create_without_job_name_is_validation_error() {
        let s = shared();
        let err = create_advanced_prompt_optimization_job(&s, &req(), &json!({}))
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn get_by_arn_or_name_or_id() {
        let s = shared();
        create_advanced_prompt_optimization_job(&s, &req(), &full_body("n1")).unwrap();
        let arn = s
            .read()
            .default_ref()
            .advanced_prompt_optimization_jobs
            .keys()
            .next()
            .unwrap()
            .clone();
        let id = arn.rsplit('/').next().unwrap().to_string();
        assert!(get_advanced_prompt_optimization_job(&s, &req(), &arn).is_ok());
        assert!(get_advanced_prompt_optimization_job(&s, &req(), &id).is_ok());
        assert!(get_advanced_prompt_optimization_job(&s, &req(), "n1").is_ok());
    }

    #[test]
    fn list_paginates() {
        let s = shared();
        for i in 0..3 {
            create_advanced_prompt_optimization_job(&s, &req(), &full_body(&format!("j{i}")))
                .unwrap();
        }
        let mut r = req();
        r.query_params
            .insert("maxResults".to_string(), "2".to_string());
        let resp = list_advanced_prompt_optimization_jobs(&s, &r).unwrap();
        let text = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        let v: Value = serde_json::from_str(text).unwrap();
        assert_eq!(v["jobSummaries"].as_array().unwrap().len(), 2);
        assert!(v["nextToken"].is_string());
    }

    #[test]
    fn stop_transitions_to_stopped() {
        let s = shared();
        create_advanced_prompt_optimization_job(&s, &req(), &full_body("j")).unwrap();
        let arn = s
            .read()
            .default_ref()
            .advanced_prompt_optimization_jobs
            .keys()
            .next()
            .unwrap()
            .clone();
        stop_advanced_prompt_optimization_job(&s, &req(), &arn).unwrap();
        assert_eq!(
            s.read().default_ref().advanced_prompt_optimization_jobs[&arn].status,
            "Stopped"
        );
    }

    #[test]
    fn stop_already_stopped_returns_conflict() {
        let s = shared();
        create_advanced_prompt_optimization_job(&s, &req(), &full_body("j")).unwrap();
        let arn = s
            .read()
            .default_ref()
            .advanced_prompt_optimization_jobs
            .keys()
            .next()
            .unwrap()
            .clone();
        stop_advanced_prompt_optimization_job(&s, &req(), &arn).unwrap();
        let err = stop_advanced_prompt_optimization_job(&s, &req(), &arn)
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn batch_delete_removes_matches_collects_errors() {
        let s = shared();
        create_advanced_prompt_optimization_job(&s, &req(), &full_body("a")).unwrap();
        create_advanced_prompt_optimization_job(&s, &req(), &full_body("b")).unwrap();
        let body = json!({"jobIdentifiers": ["a", "missing"]});
        let resp = batch_delete_advanced_prompt_optimization_job(&s, &req(), &body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(v["errors"].as_array().unwrap().len(), 1);
        assert_eq!(
            s.read()
                .default_ref()
                .advanced_prompt_optimization_jobs
                .len(),
            1
        );
    }

    #[test]
    fn batch_delete_missing_identifiers_validation_error() {
        let s = shared();
        let err = batch_delete_advanced_prompt_optimization_job(&s, &req(), &json!({}))
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }
}
