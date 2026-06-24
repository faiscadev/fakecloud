use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{InferenceProfile, SharedBedrockState};

pub(crate) fn create_inference_profile(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let default_name = crate::short_uuid();
    let profile_name = body["inferenceProfileName"]
        .as_str()
        .unwrap_or(&default_name);

    // An application inference profile gets a lowercase-alphanumeric id and an
    // `application-inference-profile/<id>` ARN (system-defined profiles use the
    // bare `inference-profile/` resource); the provider asserts this shape.
    let profile_id = Uuid::new_v4().simple().to_string()[..12].to_string();
    let profile_arn = format!(
        "arn:aws:bedrock:{}:{}:application-inference-profile/{}",
        req.region, req.account_id, profile_id
    );

    let now = Utc::now();
    let profile = InferenceProfile {
        inference_profile_arn: profile_arn.clone(),
        inference_profile_name: profile_name.to_string(),
        description: body["description"].as_str().map(|s| s.to_string()),
        model_source: body.get("modelSource").cloned().unwrap_or(json!({})),
        // AWS reports the status in upper case (`ACTIVE`); the provider's
        // creation waiter compares case-sensitively against `ACTIVE`.
        status: "ACTIVE".to_string(),
        inference_profile_type: "APPLICATION".to_string(),
        created_at: now,
        updated_at: now,
    };

    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);

    if let Some(tags) = body["tags"].as_array() {
        let tag_map: std::collections::BTreeMap<String, String> = tags
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

    Ok(AwsResponse::json_value(
        StatusCode::CREATED,
        json!({ "inferenceProfileArn": profile_arn }),
    ))
}

pub(crate) fn get_inference_profile(
    state: &SharedBedrockState,
    req: &AwsRequest,
    identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let accts = state.read();
    let empty = crate::state::BedrockState::new(&req.account_id, &req.region);
    let s = accts.get(&req.account_id).unwrap_or(&empty);
    let profile = match s.inference_profiles.get(identifier).or_else(|| {
        s.inference_profiles.values().find(|p| {
            p.inference_profile_name == identifier
                || p.inference_profile_arn.ends_with(&format!("/{identifier}"))
        })
    }) {
        Some(profile) => profile,
        None => {
            // Fall back to the AWS-managed system-defined catalogue, resolving
            // by profile id or ARN suffix.
            if let Some((id, model)) = SYSTEM_INFERENCE_PROFILES
                .iter()
                .find(|(id, _)| *id == identifier || identifier.ends_with(&format!("/{id}")))
            {
                return Ok(AwsResponse::ok_json(system_profile_json(req, id, model)));
            }
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Inference profile {identifier} not found"),
            ));
        }
    };

    // GetInferenceProfileResponse intentionally does not surface the
    // creation-time `modelSource` blob; only the resolved `models` list
    // appears on the wire.
    Ok(AwsResponse::ok_json(json!({
        "inferenceProfileArn": profile.inference_profile_arn,
        "inferenceProfileId": profile_id_from_arn(&profile.inference_profile_arn),
        "inferenceProfileName": profile.inference_profile_name,
        "description": profile.description,
        "models": profile_models(&profile.model_source),
        "status": profile.status,
        "type": profile.inference_profile_type,
        "createdAt": profile.created_at.to_rfc3339(),
        "updatedAt": profile.updated_at.to_rfc3339(),
    })))
}

pub(crate) fn list_inference_profiles(
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

    // SYSTEM_DEFINED (AWS-managed cross-region) profiles exist out of the box;
    // list them alongside any user-created APPLICATION profiles. A `type`
    // filter, when present, narrows to one kind.
    let type_filter = req
        .query_params
        .get("type")
        .or_else(|| req.query_params.get("typeEquals"));
    let include_application = type_filter.map(|t| t == "APPLICATION").unwrap_or(true);
    let include_system = type_filter.map(|t| t == "SYSTEM_DEFINED").unwrap_or(true);

    let mut summaries: Vec<Value> = Vec::new();
    if include_system {
        summaries.extend(system_profiles(req));
    }
    if include_application {
        summaries.extend(items.iter().map(|p| {
            json!({
                "inferenceProfileArn": p.inference_profile_arn,
                "inferenceProfileId": profile_id_from_arn(&p.inference_profile_arn),
                "inferenceProfileName": p.inference_profile_name,
                "description": p.description,
                "models": profile_models(&p.model_source),
                "status": p.status,
                "type": p.inference_profile_type,
                "createdAt": p.created_at.to_rfc3339(),
                "updatedAt": p.updated_at.to_rfc3339(),
            })
        }));
    }

    let total = summaries.len();
    let page: Vec<Value> = summaries
        .into_iter()
        .skip(start)
        .take(max_results)
        .collect();

    let mut resp = json!({ "inferenceProfileSummaries": page });
    let end = start.saturating_add(max_results);
    if end < total {
        // Page on the last item's id so the caller can continue.
        if let Some(last) = resp["inferenceProfileSummaries"]
            .as_array()
            .and_then(|a| a.last())
            .and_then(|v| v["inferenceProfileId"].as_str())
        {
            resp["nextToken"] = json!(last);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

/// The last segment of the inference-profile ARN is the unique ID Bedrock
/// echoes back in the summary, so we just split on `/`.
fn profile_id_from_arn(arn: &str) -> String {
    arn.rsplit('/').next().unwrap_or(arn).to_string()
}

/// AWS-managed cross-region (SYSTEM_DEFINED) inference profiles for the US
/// commercial regions. Each routes to one foundation model across us-east-1 and
/// us-west-2. Bedrock exposes these out of the box (no create call), so
/// `ListInferenceProfiles` returns them and the data sources resolve against
/// them. `(profile_id, foundation_model_id)` pairs; the `us.` prefix is the
/// geographic routing scope.
const SYSTEM_INFERENCE_PROFILES: &[(&str, &str)] = &[
    (
        "us.anthropic.claude-3-5-sonnet-20240620-v1:0",
        "anthropic.claude-3-5-sonnet-20240620-v1:0",
    ),
    (
        "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
        "anthropic.claude-3-5-sonnet-20241022-v2:0",
    ),
    (
        "us.anthropic.claude-3-5-haiku-20241022-v1:0",
        "anthropic.claude-3-5-haiku-20241022-v1:0",
    ),
    (
        "us.anthropic.claude-3-haiku-20240307-v1:0",
        "anthropic.claude-3-haiku-20240307-v1:0",
    ),
    (
        "us.anthropic.claude-3-sonnet-20240229-v1:0",
        "anthropic.claude-3-sonnet-20240229-v1:0",
    ),
    (
        "us.anthropic.claude-3-opus-20240229-v1:0",
        "anthropic.claude-3-opus-20240229-v1:0",
    ),
    ("us.amazon.nova-pro-v1:0", "amazon.nova-pro-v1:0"),
    ("us.amazon.nova-lite-v1:0", "amazon.nova-lite-v1:0"),
    ("us.amazon.nova-micro-v1:0", "amazon.nova-micro-v1:0"),
    (
        "us.meta.llama3-1-8b-instruct-v1:0",
        "meta.llama3-1-8b-instruct-v1:0",
    ),
    (
        "us.meta.llama3-1-70b-instruct-v1:0",
        "meta.llama3-1-70b-instruct-v1:0",
    ),
    (
        "us.meta.llama3-2-1b-instruct-v1:0",
        "meta.llama3-2-1b-instruct-v1:0",
    ),
    (
        "us.meta.llama3-2-3b-instruct-v1:0",
        "meta.llama3-2-3b-instruct-v1:0",
    ),
    (
        "us.meta.llama3-2-11b-instruct-v1:0",
        "meta.llama3-2-11b-instruct-v1:0",
    ),
    (
        "us.meta.llama3-2-90b-instruct-v1:0",
        "meta.llama3-2-90b-instruct-v1:0",
    ),
];

/// The US commercial regions a `us.`-scoped system profile routes across.
const SYSTEM_PROFILE_REGIONS: &[&str] = &["us-east-1", "us-west-2"];

/// Build the JSON summary for one system-defined inference profile.
fn system_profile_json(req: &AwsRequest, profile_id: &str, model_id: &str) -> Value {
    let arn = format!(
        "arn:aws:bedrock:{}:{}:inference-profile/{}",
        req.region, req.account_id, profile_id
    );
    let models: Vec<Value> = SYSTEM_PROFILE_REGIONS
        .iter()
        .map(|region| {
            json!({
                "modelArn": format!("arn:aws:bedrock:{region}::foundation-model/{model_id}")
            })
        })
        .collect();
    json!({
        "inferenceProfileArn": arn,
        "inferenceProfileId": profile_id,
        "inferenceProfileName": profile_id,
        "description": format!("Routes requests to {model_id} across US regions."),
        "models": models,
        "status": "ACTIVE",
        "type": "SYSTEM_DEFINED",
        "createdAt": "2024-01-01T00:00:00+00:00",
        "updatedAt": "2024-01-01T00:00:00+00:00",
    })
}

/// All system-defined inference-profile summaries, keyed for lookup by id/arn.
fn system_profiles(req: &AwsRequest) -> Vec<Value> {
    SYSTEM_INFERENCE_PROFILES
        .iter()
        .map(|(id, model)| system_profile_json(req, id, model))
        .collect()
}

/// Build the `models` summary list. We try to extract a copyFrom model ARN
/// from the user-supplied `modelSource`, otherwise emit one synthesized entry
/// pointing at a real foundation-model ARN so the response satisfies the
/// `min: 1` length constraint on `InferenceProfileModels`.
fn profile_models(model_source: &Value) -> Value {
    if let Some(copy_from) = model_source.get("copyFrom").and_then(Value::as_str) {
        return json!([{ "modelArn": copy_from }]);
    }
    json!([{
        "modelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1"
    }])
}

pub(crate) fn delete_inference_profile(
    state: &SharedBedrockState,
    req: &AwsRequest,
    identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);

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
            MultiAccountState::new("123456789012", "us-east-1", "http://x");
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

    fn create(state: &SharedBedrockState, name: &str, with_tags: bool) -> String {
        let body = if with_tags {
            json!({
                "inferenceProfileName": name,
                "tags": [{"key": "env", "value": "prod"}]
            })
        } else {
            json!({"inferenceProfileName": name})
        };
        let resp = create_inference_profile(state, &req(), &body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        v["inferenceProfileArn"].as_str().unwrap().to_string()
    }

    #[test]
    fn create_with_tags_records_tag_map() {
        let s = shared();
        let arn = create(&s, "p1", true);
        let state = s.read();
        let accts = state.default_ref();
        assert!(accts.tags.contains_key(&arn));
    }

    #[test]
    fn create_without_tags_skips_tag_map() {
        let s = shared();
        let arn = create(&s, "p2", false);
        let state = s.read();
        let accts = state.default_ref();
        assert!(!accts.tags.contains_key(&arn));
    }

    #[test]
    fn get_by_arn_name_and_id() {
        let s = shared();
        let arn = create(&s, "p-look", false);
        let id = arn.rsplit('/').next().unwrap().to_string();
        assert!(get_inference_profile(&s, &req(), &arn).is_ok());
        assert!(get_inference_profile(&s, &req(), &id).is_ok());
        assert!(get_inference_profile(&s, &req(), "p-look").is_ok());
    }

    #[test]
    fn get_unknown_returns_not_found() {
        let s = shared();
        let err = get_inference_profile(&s, &req(), "missing").err().unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn list_paginates() {
        let s = shared();
        for i in 0..3 {
            create(&s, &format!("p{i}"), false);
        }
        let mut r = req();
        r.query_params
            .insert("maxResults".to_string(), "2".to_string());
        let resp = list_inference_profiles(&s, &r).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(v["inferenceProfileSummaries"].as_array().unwrap().len(), 2);
        assert!(v["nextToken"].is_string());
    }

    #[test]
    fn delete_removes_entry() {
        let s = shared();
        let arn = create(&s, "del", false);
        delete_inference_profile(&s, &req(), &arn).unwrap();
        assert!(s.read().default_ref().inference_profiles.is_empty());
    }

    #[test]
    fn delete_unknown_returns_not_found() {
        let s = shared();
        let err = delete_inference_profile(&s, &req(), "missing")
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }
}
