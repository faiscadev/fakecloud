use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::state::SharedBedrockState;

pub fn put_resource_policy(
    state: &SharedBedrockState,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let resource_arn = body["resourceArn"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "resourceArn is required",
        )
    })?;

    let policy = body["resourcePolicy"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "resourcePolicy is required",
        )
    })?;

    let revision_id = Uuid::new_v4().to_string();

    let mut s = state.write();
    s.resource_policies
        .insert(resource_arn.to_string(), policy.to_string());

    Ok(AwsResponse::ok_json(json!({
        "resourceArn": resource_arn,
        "revisionId": revision_id,
    })))
}

pub fn get_resource_policy(
    state: &SharedBedrockState,
    resource_arn: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let policy = s
        .resource_policies
        .get(resource_arn)
        .or_else(|| {
            s.resource_policies
                .iter()
                .find(|(k, _)| k.ends_with(&format!("/{resource_arn}")))
                .map(|(_, v)| v)
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Resource policy for {resource_arn} not found"),
            )
        })?;

    let revision_id = Uuid::new_v4().to_string();

    Ok(AwsResponse::ok_json(json!({
        "resourcePolicy": policy,
        "revisionId": revision_id,
    })))
}

pub fn delete_resource_policy(
    state: &SharedBedrockState,
    resource_arn: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let key = if s.resource_policies.contains_key(resource_arn) {
        Some(resource_arn.to_string())
    } else {
        s.resource_policies
            .keys()
            .find(|k| k.ends_with(&format!("/{resource_arn}")))
            .cloned()
    };

    match key {
        Some(k) => {
            s.resource_policies.remove(&k);
            Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
        }
        None => Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Resource policy for {resource_arn} not found"),
        )),
    }
}
