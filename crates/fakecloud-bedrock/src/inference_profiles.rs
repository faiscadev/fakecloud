use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{InferenceProfile, SharedBedrockState};

pub fn create_inference_profile(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let default_name = Uuid::new_v4().to_string()[..8].to_string();
    let profile_name = body["inferenceProfileName"]
        .as_str()
        .unwrap_or(&default_name);

    let profile_id = Uuid::new_v4().to_string();
    let profile_arn = format!(
        "arn:aws:bedrock:{}:{}:inference-profile/{}",
        req.region, req.account_id, profile_id
    );

    let now = Utc::now();
    let profile = InferenceProfile {
        inference_profile_arn: profile_arn.clone(),
        inference_profile_name: profile_name.to_string(),
        description: body["description"].as_str().map(|s| s.to_string()),
        model_source: body.get("modelSource").cloned().unwrap_or(json!({})),
        status: "Active".to_string(),
        inference_profile_type: "APPLICATION".to_string(),
        created_at: now,
        updated_at: now,
    };

    let mut s = state.write();

    if let Some(tags) = body["tags"].as_array() {
        let tag_map: std::collections::HashMap<String, String> = tags
            .iter()
            .filter_map(|t| {
                Some((
                    t["key"].as_str()?.to_string(),
                    t["value"].as_str()?.to_string(),
                ))
            })
            .collect();
        if !tag_map.is_empty() {
            s.tags.insert(profile_arn.clone(), tag_map);
        }
    }

    s.inference_profiles.insert(profile_arn.clone(), profile);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "inferenceProfileArn": profile_arn })).unwrap(),
    ))
}

pub fn get_inference_profile(
    state: &SharedBedrockState,
    identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let profile = s
        .inference_profiles
        .get(identifier)
        .or_else(|| {
            s.inference_profiles.values().find(|p| {
                p.inference_profile_name == identifier
                    || p.inference_profile_arn.ends_with(&format!("/{identifier}"))
            })
        })
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Inference profile {identifier} not found"),
            )
        })?;

    Ok(AwsResponse::ok_json(json!({
        "inferenceProfileArn": profile.inference_profile_arn,
        "inferenceProfileName": profile.inference_profile_name,
        "description": profile.description,
        "modelSource": profile.model_source,
        "status": profile.status,
        "type": profile.inference_profile_type,
        "createdAt": profile.created_at.to_rfc3339(),
        "updatedAt": profile.updated_at.to_rfc3339(),
    })))
}

pub fn list_inference_profiles(
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
    let mut items: Vec<&InferenceProfile> = s.inference_profiles.values().collect();
    items.sort_by(|a, b| a.inference_profile_arn.cmp(&b.inference_profile_arn));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|p| p.inference_profile_arn.as_str() > token.as_str())
            .unwrap_or(items.len())
    } else {
        0
    };

    let page: Vec<Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|p| {
            json!({
                "inferenceProfileArn": p.inference_profile_arn,
                "inferenceProfileName": p.inference_profile_name,
                "description": p.description,
                "status": p.status,
                "type": p.inference_profile_type,
                "createdAt": p.created_at.to_rfc3339(),
                "updatedAt": p.updated_at.to_rfc3339(),
            })
        })
        .collect();

    let mut resp = json!({ "inferenceProfileSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.inference_profile_arn);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn delete_inference_profile(
    state: &SharedBedrockState,
    identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let key = s
        .inference_profiles
        .iter()
        .find(|(k, p)| {
            *k == identifier
                || p.inference_profile_name == identifier
                || p.inference_profile_arn.ends_with(&format!("/{identifier}"))
        })
        .map(|(k, _)| k.clone());

    match key {
        Some(k) => {
            s.inference_profiles.remove(&k);
            Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
        }
        None => Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Inference profile {identifier} not found"),
        )),
    }
}
