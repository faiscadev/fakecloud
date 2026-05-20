use chrono::Utc;
use fakecloud_aws::arn::Arn;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::SharedBedrockState;

pub(crate) fn create_model_customization_job(
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
                    .map(|(k, v)| {
                        let val = match v.as_str() {
                            Some(s) => s.to_string(),
                            None => v.to_string(), // serialize non-string values
                        };
                        (k.clone(), val)
                    })
                    .collect()
            })
            .unwrap_or_default(),
        status: "InProgress".to_string(),
        created_at: now,
        last_modified_at: now,
    };

    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    s.customization_jobs.insert(job_arn.clone(), job);

    Ok(AwsResponse::json_value(
        StatusCode::CREATED,
        json!({ "jobArn": job_arn }),
    ))
}

pub(crate) fn get_model_customization_job(
    state: &SharedBedrockState,
    req: &AwsRequest,
    job_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let accts = state.read();
    let empty = crate::state::BedrockState::new(&req.account_id, &req.region);
    let s = accts.get(&req.account_id).unwrap_or(&empty);

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

    // GetModelCustomizationJobResponse requires `baseModelArn` and
    // `validationDataConfig`; the stored `baseModelIdentifier` is normalized
    // into a real foundation-model ARN below, and a minimal empty
    // `validationDataConfig` is surfaced so the wire shape stays valid.
    Ok(AwsResponse::ok_json(json!({
        "jobArn": job.job_arn,
        "jobName": job.job_name,
        "outputModelName": job.custom_model_name,
        "outputModelArn": output_model_arn,
        "roleArn": job.role_arn,
        "status": job.status,
        "creationTime": job.created_at.to_rfc3339(),
        "lastModifiedTime": job.last_modified_at.to_rfc3339(),
        "baseModelArn": base_model_arn(&job.base_model_identifier, &req.region),
        "trainingDataConfig": job.training_data_config,
        "validationDataConfig": json!({ "validators": [] }),
        "outputDataConfig": job.output_data_config,
        "hyperParameters": job.hyper_parameters,
    })))
}

pub(crate) fn list_model_customization_jobs(
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

    let accts = state.read();
    let empty = crate::state::BedrockState::new(&req.account_id, &req.region);
    let s = accts.get(&req.account_id).unwrap_or(&empty);
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
                "baseModelArn": base_model_arn(&j.base_model_identifier, &req.region),
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

/// Normalize the customization job's stored base model identifier into a
/// full foundation-model ARN, since the Smithy summary requires `baseModelArn`.
fn base_model_arn(base_model_identifier: &str, region: &str) -> String {
    if base_model_identifier.starts_with("arn:") {
        base_model_identifier.to_string()
    } else {
        Arn::new(
            "bedrock",
            region,
            "",
            &format!("foundation-model/{base_model_identifier}"),
        )
        .to_string()
    }
}

pub(crate) fn stop_model_customization_job(
    state: &SharedBedrockState,
    req: &AwsRequest,
    job_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);

    let job = s
        .customization_jobs
        .iter_mut()
        .find_map(|(k, j)| {
            if *k == job_identifier || j.job_name == job_identifier {
                Some(j)
            } else {
                None
            }
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Model customization job {job_identifier} not found"),
            )
        })?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::BedrockState;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn shared() -> SharedBedrockState {
        let multi: MultiAccountState<BedrockState> =
            MultiAccountState::new("123456789012", "us-east-1", "http://localhost");
        Arc::new(RwLock::new(multi))
    }

    fn req() -> AwsRequest {
        AwsRequest {
            service: "bedrock".to_string(),
            action: "a".to_string(),
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
            request_id: "r".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn create_job_requires_name() {
        let s = shared();
        let err = create_model_customization_job(&s, &req(), &json!({}))
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn create_job_stores_record_with_defaults() {
        let s = shared();
        create_model_customization_job(
            &s,
            &req(),
            &json!({"jobName": "j", "roleArn": "role", "hyperParameters": {"lr": "0.01", "epochs": 3}}),
        )
        .unwrap();
        let accts = s.read();
        let state = accts.default_ref();
        assert_eq!(state.customization_jobs.len(), 1);
        let job = state.customization_jobs.values().next().unwrap();
        assert_eq!(job.job_name, "j");
        assert_eq!(job.custom_model_name, "j");
        assert_eq!(job.status, "InProgress");
        assert_eq!(
            job.hyper_parameters.get("lr").map(String::as_str),
            Some("0.01")
        );
        assert_eq!(
            job.hyper_parameters.get("epochs").map(String::as_str),
            Some("3")
        );
    }

    #[test]
    fn get_job_by_arn_and_by_name() {
        let s = shared();
        create_model_customization_job(&s, &req(), &json!({"jobName": "my-job"})).unwrap();
        let arn = s
            .read()
            .default_ref()
            .customization_jobs
            .keys()
            .next()
            .unwrap()
            .clone();
        assert!(get_model_customization_job(&s, &req(), &arn).is_ok());
        assert!(get_model_customization_job(&s, &req(), "my-job").is_ok());
    }

    #[test]
    fn get_job_unknown_not_found() {
        let s = shared();
        let err = get_model_customization_job(&s, &req(), "missing")
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn list_jobs_paginates() {
        let s = shared();
        for i in 0..3 {
            create_model_customization_job(&s, &req(), &json!({"jobName": format!("j{i}")}))
                .unwrap();
        }
        let mut r = req();
        r.query_params
            .insert("maxResults".to_string(), "2".to_string());
        let resp = list_model_customization_jobs(&s, &r).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(
            v["modelCustomizationJobSummaries"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert!(v["nextToken"].is_string());
    }

    #[test]
    fn stop_job_transitions_status() {
        let s = shared();
        create_model_customization_job(&s, &req(), &json!({"jobName": "j"})).unwrap();
        let arn = s
            .read()
            .default_ref()
            .customization_jobs
            .keys()
            .next()
            .unwrap()
            .clone();
        stop_model_customization_job(&s, &req(), &arn).unwrap();
        assert_eq!(
            s.read().default_ref().customization_jobs[&arn].status,
            "Stopped"
        );
    }

    #[test]
    fn stop_job_not_in_progress_conflicts() {
        let s = shared();
        create_model_customization_job(&s, &req(), &json!({"jobName": "j"})).unwrap();
        let arn = s
            .read()
            .default_ref()
            .customization_jobs
            .keys()
            .next()
            .unwrap()
            .clone();
        stop_model_customization_job(&s, &req(), &arn).unwrap();
        let err = stop_model_customization_job(&s, &req(), &arn)
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn stop_job_unknown_not_found() {
        let s = shared();
        let err = stop_model_customization_job(&s, &req(), "missing")
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }
}
