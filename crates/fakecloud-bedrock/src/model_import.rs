use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{ImportedModel, ModelImportJob, SharedBedrockState};

pub fn create_model_import_job(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let job_name = body["jobName"].as_str().unwrap_or("import-job");
    let imported_model_name = body["importedModelName"].as_str().unwrap_or(job_name);
    let role_arn = body["roleArn"].as_str().unwrap_or_default();

    let job_id = Uuid::new_v4().to_string();
    let job_arn = format!(
        "arn:aws:bedrock:{}:{}:model-import-job/{}",
        req.region, req.account_id, job_id
    );
    let imported_model_arn = format!(
        "arn:aws:bedrock:{}:{}:imported-model/{}",
        req.region, req.account_id, imported_model_name
    );

    let now = Utc::now();
    let model_data_source = body.get("modelDataSource").cloned().unwrap_or(json!({}));

    let job = ModelImportJob {
        job_arn: job_arn.clone(),
        job_name: job_name.to_string(),
        imported_model_name: imported_model_name.to_string(),
        imported_model_arn: imported_model_arn.clone(),
        role_arn: role_arn.to_string(),
        model_data_source: model_data_source.clone(),
        status: "Completed".to_string(),
        creation_time: now,
        last_modified_time: now,
    };

    let imported_model = ImportedModel {
        model_arn: imported_model_arn.clone(),
        model_name: imported_model_name.to_string(),
        job_arn: job_arn.clone(),
        model_data_source,
        creation_time: now,
    };

    let mut s = state.write();
    s.model_import_jobs.insert(job_arn.clone(), job);
    s.imported_models
        .insert(imported_model_arn.clone(), imported_model);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "jobArn": job_arn })).unwrap(),
    ))
}

pub fn get_model_import_job(
    state: &SharedBedrockState,
    job_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let job = s
        .model_import_jobs
        .get(job_identifier)
        .or_else(|| {
            s.model_import_jobs.values().find(|j| {
                j.job_name == job_identifier || j.job_arn.ends_with(&format!("/{job_identifier}"))
            })
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Model import job {job_identifier} not found"),
            )
        })?;

    Ok(AwsResponse::ok_json(json!({
        "jobArn": job.job_arn,
        "jobName": job.job_name,
        "importedModelName": job.imported_model_name,
        "importedModelArn": job.imported_model_arn,
        "roleArn": job.role_arn,
        "modelDataSource": job.model_data_source,
        "status": job.status,
        "creationTime": job.creation_time.to_rfc3339(),
        "lastModifiedTime": job.last_modified_time.to_rfc3339(),
    })))
}

pub fn list_model_import_jobs(
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
    let mut items: Vec<&ModelImportJob> = s.model_import_jobs.values().collect();
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
                "importedModelName": j.imported_model_name,
                "importedModelArn": j.imported_model_arn,
                "creationTime": j.creation_time.to_rfc3339(),
                "lastModifiedTime": j.last_modified_time.to_rfc3339(),
            })
        })
        .collect();

    let mut resp = json!({ "modelImportJobSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.job_arn);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn get_imported_model(
    state: &SharedBedrockState,
    model_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let model = s
        .imported_models
        .get(model_identifier)
        .or_else(|| {
            s.imported_models.values().find(|m| {
                m.model_name == model_identifier
                    || m.model_arn.ends_with(&format!("/{model_identifier}"))
            })
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Imported model {model_identifier} not found"),
            )
        })?;

    Ok(AwsResponse::ok_json(json!({
        "modelArn": model.model_arn,
        "modelName": model.model_name,
        "jobArn": model.job_arn,
        "modelDataSource": model.model_data_source,
        "creationTime": model.creation_time.to_rfc3339(),
    })))
}

pub fn list_imported_models(
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
    let mut items: Vec<&ImportedModel> = s.imported_models.values().collect();
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

pub fn delete_imported_model(
    state: &SharedBedrockState,
    model_identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
    let key = s
        .imported_models
        .iter()
        .find(|(k, m)| {
            *k == model_identifier
                || m.model_name == model_identifier
                || m.model_arn.ends_with(&format!("/{model_identifier}"))
        })
        .map(|(k, _)| k.clone());

    match key {
        Some(k) => {
            s.imported_models.remove(&k);
            Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
        }
        None => Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Imported model {model_identifier} not found"),
        )),
    }
}
