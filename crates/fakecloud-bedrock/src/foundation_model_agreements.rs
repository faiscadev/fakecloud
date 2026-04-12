use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{FoundationModelAgreement, SharedBedrockState};

pub fn create_foundation_model_agreement(
    state: &SharedBedrockState,
    _req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let model_id = body["modelId"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "modelId is required",
        )
    })?;

    let agreement_id = Uuid::new_v4().to_string();

    let agreement = FoundationModelAgreement {
        agreement_id: agreement_id.clone(),
        model_id: model_id.to_string(),
        created_at: Utc::now(),
    };

    let mut s = state.write();
    s.foundation_model_agreements
        .insert(agreement_id, agreement);

    Ok(AwsResponse::ok_json(json!({
        "modelId": model_id,
    })))
}

pub fn delete_foundation_model_agreement(
    state: &SharedBedrockState,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let model_id = body["modelId"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "modelId is required",
        )
    })?;

    let mut s = state.write();
    let key = s
        .foundation_model_agreements
        .iter()
        .find(|(_, a)| a.model_id == model_id)
        .map(|(k, _)| k.clone());

    match key {
        Some(k) => {
            s.foundation_model_agreements.remove(&k);
            Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
        }
        None => Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Foundation model agreement for {model_id} not found"),
        )),
    }
}

pub fn list_foundation_model_agreement_offers(
    _state: &SharedBedrockState,
    model_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::ok_json(json!({
        "modelId": model_id,
        "offers": [],
    })))
}

pub fn get_foundation_model_availability(
    state: &SharedBedrockState,
    model_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let has_agreement = s
        .foundation_model_agreements
        .values()
        .any(|a| a.model_id == model_id);

    Ok(AwsResponse::ok_json(json!({
        "modelId": model_id,
        "agreementAvailability": {
            "status": if has_agreement { "AVAILABLE" } else { "NOT_AVAILABLE" },
        },
    })))
}

pub fn get_use_case_for_model_access(
    state: &SharedBedrockState,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let use_case = s.use_case_for_model_access.clone().unwrap_or(json!(null));

    Ok(AwsResponse::ok_json(json!({
        "useCase": use_case,
    })))
}

pub fn put_use_case_for_model_access(
    state: &SharedBedrockState,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let use_case = body.get("useCase").cloned().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "useCase is required",
        )
    })?;

    let mut s = state.write();
    s.use_case_for_model_access = Some(use_case);

    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}
