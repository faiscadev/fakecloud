use chrono::Utc;
use http::StatusCode;
use regex::Regex;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{Guardrail, GuardrailVersion, SharedBedrockState};

/// Normalize a guardrail identifier to its storage key. The API accepts either
/// the bare guardrail id or the full ARN (`arn:...:guardrail/<id>`); operations
/// such as CreateGuardrailVersion are called by the provider with the ARN.
fn guardrail_key(identifier: &str) -> &str {
    identifier
        .rsplit_once("guardrail/")
        .map(|(_, id)| id)
        .unwrap_or(identifier)
}

pub(crate) fn create_guardrail(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let name = body["name"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "name is required",
        )
    })?;

    let blocked_input_messaging = body["blockedInputMessaging"]
        .as_str()
        .unwrap_or("Sorry, the model cannot answer this question.")
        .to_string();
    let blocked_outputs_messaging = body["blockedOutputsMessaging"]
        .as_str()
        .unwrap_or("Sorry, the model cannot answer this question.")
        .to_string();

    let guardrail_id = crate::short_uuid();
    let guardrail_arn = format!(
        "arn:aws:bedrock:{}:{}:guardrail/{}",
        req.region, req.account_id, guardrail_id
    );

    let now = Utc::now();
    let guardrail = Guardrail {
        guardrail_id: guardrail_id.clone(),
        guardrail_arn: guardrail_arn.clone(),
        name: name.to_string(),
        description: body["description"].as_str().unwrap_or("").to_string(),
        status: "READY".to_string(),
        version: "DRAFT".to_string(),
        next_version_number: 1,
        blocked_input_messaging,
        blocked_outputs_messaging,
        content_policy: body.get("contentPolicyConfig").cloned(),
        word_policy: body.get("wordPolicyConfig").cloned(),
        sensitive_information_policy: body.get("sensitiveInformationPolicyConfig").cloned(),
        topic_policy: body.get("topicPolicyConfig").cloned(),
        contextual_grounding_policy: body.get("contextualGroundingPolicyConfig").cloned(),
        created_at: now,
        updated_at: now,
    };

    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    s.guardrails.insert(guardrail_id.clone(), guardrail);

    Ok(AwsResponse::json_value(
        StatusCode::CREATED,
        json!({
            "guardrailId": guardrail_id,
            "guardrailArn": guardrail_arn,
            "version": "DRAFT",
            "createdAt": now.to_rfc3339(),
        }),
    ))
}

pub(crate) fn get_guardrail(
    state: &SharedBedrockState,
    req: &AwsRequest,
    guardrail_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let guardrail_id = guardrail_key(guardrail_id);
    // Check if a specific version is requested
    let version = req.query_params.get("guardrailVersion");

    let accts = state.read();
    let empty = crate::state::BedrockState::new(&req.account_id, &req.region);
    let s = accts.get(&req.account_id).unwrap_or(&empty);

    // If a numbered version was requested, look it up in versions
    if let Some(ver) = version {
        if ver != "DRAFT" {
            let key = (guardrail_id.to_string(), ver.clone());
            if let Some(gv) = s.guardrail_versions.get(&key) {
                return Ok(AwsResponse::ok_json(guardrail_version_to_json(gv)));
            }
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Guardrail version {ver} not found for {guardrail_id}"),
            ));
        }
    }

    let guardrail = s.guardrails.get(guardrail_id).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Guardrail {guardrail_id} not found"),
        )
    })?;

    Ok(AwsResponse::ok_json(guardrail_to_json(guardrail)))
}

pub(crate) fn list_guardrails(
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
    let mut items: Vec<&Guardrail> = s.guardrails.values().collect();
    items.sort_by(|a, b| a.guardrail_id.cmp(&b.guardrail_id));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|g| g.guardrail_id.as_str() > token.as_str())
            .unwrap_or(items.len())
    } else {
        0
    };

    let page: Vec<Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|g| {
            json!({
                "id": g.guardrail_id,
                "arn": g.guardrail_arn,
                "name": g.name,
                "description": g.description,
                "status": g.status,
                "version": g.version,
                "createdAt": g.created_at.to_rfc3339(),
                "updatedAt": g.updated_at.to_rfc3339(),
            })
        })
        .collect();

    let mut resp = json!({ "guardrails": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.guardrail_id);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub(crate) fn update_guardrail(
    state: &SharedBedrockState,
    req: &AwsRequest,
    guardrail_id: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let guardrail_id = guardrail_key(guardrail_id);
    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    let guardrail = s.guardrails.get_mut(guardrail_id).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Guardrail {guardrail_id} not found"),
        )
    })?;

    if let Some(name) = body["name"].as_str() {
        guardrail.name = name.to_string();
    }
    if let Some(desc) = body["description"].as_str() {
        guardrail.description = desc.to_string();
    }
    if let Some(msg) = body["blockedInputMessaging"].as_str() {
        guardrail.blocked_input_messaging = msg.to_string();
    }
    if let Some(msg) = body["blockedOutputsMessaging"].as_str() {
        guardrail.blocked_outputs_messaging = msg.to_string();
    }
    if let Some(policy) = body.get("contentPolicyConfig") {
        guardrail.content_policy = Some(policy.clone());
    }
    if let Some(policy) = body.get("wordPolicyConfig") {
        guardrail.word_policy = Some(policy.clone());
    }
    if let Some(policy) = body.get("sensitiveInformationPolicyConfig") {
        guardrail.sensitive_information_policy = Some(policy.clone());
    }
    if let Some(policy) = body.get("topicPolicyConfig") {
        guardrail.topic_policy = Some(policy.clone());
    }
    if let Some(policy) = body.get("contextualGroundingPolicyConfig") {
        guardrail.contextual_grounding_policy = Some(policy.clone());
    }

    guardrail.updated_at = Utc::now();

    let resp = json!({
        "guardrailId": guardrail.guardrail_id,
        "guardrailArn": guardrail.guardrail_arn,
        "version": guardrail.version,
        "updatedAt": guardrail.updated_at.to_rfc3339(),
    });

    Ok(AwsResponse::ok_json(resp))
}

pub(crate) fn delete_guardrail(
    state: &SharedBedrockState,
    req: &AwsRequest,
    guardrail_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let guardrail_id = guardrail_key(guardrail_id);
    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    s.guardrails.remove(guardrail_id).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Guardrail {guardrail_id} not found"),
        )
    })?;

    // Remove all versions
    s.guardrail_versions.retain(|(id, _), _| id != guardrail_id);

    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}

pub(crate) fn create_guardrail_version(
    state: &SharedBedrockState,
    req: &AwsRequest,
    guardrail_id: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let guardrail_id = guardrail_key(guardrail_id);
    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    let guardrail = s.guardrails.get_mut(guardrail_id).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Guardrail {guardrail_id} not found"),
        )
    })?;

    let version_number = guardrail.next_version_number;
    guardrail.next_version_number += 1;
    let version_str = version_number.to_string();

    let description = body["description"]
        .as_str()
        .unwrap_or(&guardrail.description)
        .to_string();

    let now = Utc::now();
    let version = GuardrailVersion {
        guardrail_id: guardrail_id.to_string(),
        guardrail_arn: guardrail.guardrail_arn.clone(),
        version: version_str.clone(),
        name: guardrail.name.clone(),
        description,
        status: "READY".to_string(),
        blocked_input_messaging: guardrail.blocked_input_messaging.clone(),
        blocked_outputs_messaging: guardrail.blocked_outputs_messaging.clone(),
        content_policy: guardrail.content_policy.clone(),
        word_policy: guardrail.word_policy.clone(),
        sensitive_information_policy: guardrail.sensitive_information_policy.clone(),
        topic_policy: guardrail.topic_policy.clone(),
        contextual_grounding_policy: guardrail.contextual_grounding_policy.clone(),
        created_at: now,
    };

    let key = (guardrail_id.to_string(), version_str.clone());
    s.guardrail_versions.insert(key, version);

    Ok(AwsResponse::json_value(
        StatusCode::CREATED,
        json!({
            "guardrailId": guardrail_id,
            "version": version_str,
        }),
    ))
}

/// Handle the ApplyGuardrail API — evaluate content against a guardrail.
pub(crate) fn apply_guardrail(
    state: &SharedBedrockState,
    req: &AwsRequest,
    guardrail_id: &str,
    guardrail_version: &str,
    body: &[u8],
) -> Result<AwsResponse, AwsServiceError> {
    let guardrail_id = guardrail_key(guardrail_id);
    let input: Value = serde_json::from_slice(body).unwrap_or_default();

    let accts = state.read();
    let empty = crate::state::BedrockState::new(&req.account_id, &req.region);
    let s = accts.get(&req.account_id).unwrap_or(&empty);

    // Borrow a GuardrailView over DRAFT or a numbered version, avoiding
    // the 13-field clone needed to synthesize a temporary Guardrail.
    let not_found_err = || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Guardrail {guardrail_id} version {guardrail_version} not found"),
        )
    };

    let view = if guardrail_version == "DRAFT" {
        let g = s.guardrails.get(guardrail_id).ok_or_else(not_found_err)?;
        GuardrailView::from_guardrail(g)
    } else {
        let key = (guardrail_id.to_string(), guardrail_version.to_string());
        let gv = s.guardrail_versions.get(&key).ok_or_else(not_found_err)?;
        GuardrailView::from_version(gv)
    };

    // Extract text from content blocks
    // Content blocks can be: {"text": {"text": "..."}} (GuardrailTextBlock union variant)
    // or {"text": "..."} (simple text)
    let content_blocks = input["content"].as_array();
    let mut all_text = String::new();
    if let Some(blocks) = content_blocks {
        for block in blocks {
            let text_str = block["text"]["text"]
                .as_str()
                .or_else(|| block["text"].as_str());
            if let Some(text) = text_str {
                if !all_text.is_empty() {
                    all_text.push(' ');
                }
                all_text.push_str(text);
            }
        }
    }

    let assessments = evaluate_content_view(&view, &all_text);
    let action = if assessments.is_empty() {
        "NONE"
    } else {
        "GUARDRAIL_INTERVENED"
    };

    let source = input["source"].as_str().unwrap_or("INPUT");
    let outputs = if action == "GUARDRAIL_INTERVENED" {
        let msg = if source == "INPUT" {
            view.blocked_input_messaging
        } else {
            view.blocked_outputs_messaging
        };
        vec![json!({"text": msg})]
    } else {
        content_blocks
            .map(|blocks| {
                blocks
                    .iter()
                    .filter_map(|b| {
                        b["text"]["text"]
                            .as_str()
                            .or_else(|| b["text"].as_str())
                            .map(|t| json!({"text": t}))
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    let resp = json!({
        "usage": {
            "topicPolicyUnits": 1,
            "contentPolicyUnits": 1,
            "wordPolicyUnits": 1,
            "sensitiveInformationPolicyUnits": 1,
            "sensitiveInformationPolicyFreeUnits": 0
        },
        "action": action,
        "outputs": outputs,
        "assessments": assessments,
    });

    Ok(AwsResponse::ok_json(resp))
}

// ── Content evaluation ─────────────────────────────────────────────

/// Borrowed projection over the subset of a `Guardrail` or
/// `GuardrailVersion` that content evaluation needs. Used instead of
/// cloning 13 fields into a temporary `Guardrail` inside
/// `apply_guardrail`.
pub struct GuardrailView<'a> {
    pub word_policy: Option<&'a Value>,
    pub topic_policy: Option<&'a Value>,
    pub sensitive_information_policy: Option<&'a Value>,
    pub blocked_input_messaging: &'a str,
    pub blocked_outputs_messaging: &'a str,
}

impl<'a> GuardrailView<'a> {
    pub(crate) fn from_guardrail(g: &'a Guardrail) -> Self {
        Self {
            word_policy: g.word_policy.as_ref(),
            topic_policy: g.topic_policy.as_ref(),
            sensitive_information_policy: g.sensitive_information_policy.as_ref(),
            blocked_input_messaging: &g.blocked_input_messaging,
            blocked_outputs_messaging: &g.blocked_outputs_messaging,
        }
    }

    pub(crate) fn from_version(gv: &'a GuardrailVersion) -> Self {
        Self {
            word_policy: gv.word_policy.as_ref(),
            topic_policy: gv.topic_policy.as_ref(),
            sensitive_information_policy: gv.sensitive_information_policy.as_ref(),
            blocked_input_messaging: &gv.blocked_input_messaging,
            blocked_outputs_messaging: &gv.blocked_outputs_messaging,
        }
    }
}

/// Evaluate content against a guardrail's configured policies.
/// Returns a list of assessment results.
///
/// This is a test convenience around `evaluate_content_view` that takes a
/// `Guardrail` directly. The operational call path uses
/// `evaluate_content_view` with a `GuardrailView` built from a versioned
/// guardrail. Compiled out of release builds via `cfg(test)` because the
/// non-versioned form is only useful in unit tests.
#[cfg(test)]
fn evaluate_content(guardrail: &Guardrail, text: &str) -> Vec<Value> {
    evaluate_content_view(&GuardrailView::from_guardrail(guardrail), text)
}

fn evaluate_content_view(guardrail: &GuardrailView<'_>, text: &str) -> Vec<Value> {
    let mut assessments = Vec::new();

    // Word policy evaluation
    if let Some(word_policy) = guardrail.word_policy {
        if let Some(words) = word_policy.get("wordsConfig").and_then(|w| w.as_array()) {
            for word_entry in words {
                if let Some(word) = word_entry["text"].as_str() {
                    if text.to_lowercase().contains(&word.to_lowercase()) {
                        assessments.push(json!({
                            "wordPolicy": {
                                "customWords": [{
                                    "match": word,
                                    "action": "BLOCKED"
                                }]
                            }
                        }));
                    }
                }
            }
        }
        if let Some(managed) = word_policy
            .get("managedWordListsConfig")
            .and_then(|m| m.as_array())
        {
            for entry in managed {
                if entry["type"].as_str() == Some("PROFANITY") {
                    let profanity_words = ["damn", "hell", "shit", "fuck", "ass"];
                    let text_lower = text.to_lowercase();
                    for word in &profanity_words {
                        if word_boundary_match(&text_lower, word) {
                            assessments.push(json!({
                                "wordPolicy": {
                                    "managedWordLists": [{
                                        "match": word,
                                        "type": "PROFANITY",
                                        "action": "BLOCKED"
                                    }]
                                }
                            }));
                            break;
                        }
                    }
                }
            }
        }
    }

    // Topic policy evaluation
    if let Some(topic_policy) = guardrail.topic_policy {
        if let Some(topics) = topic_policy.get("topicsConfig").and_then(|t| t.as_array()) {
            for topic in topics {
                let topic_name = topic["name"].as_str().unwrap_or("");
                if let Some(examples) = topic["examples"].as_array() {
                    for example in examples {
                        if let Some(ex) = example.as_str() {
                            if text.to_lowercase().contains(&ex.to_lowercase()) {
                                assessments.push(json!({
                                    "topicPolicy": {
                                        "topics": [{
                                            "name": topic_name,
                                            "type": "DENY",
                                            "action": "BLOCKED"
                                        }]
                                    }
                                }));
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    // Sensitive information policy evaluation (PII detection)
    if let Some(pii_policy) = guardrail.sensitive_information_policy {
        if let Some(pii_entities) = pii_policy
            .get("piiEntitiesConfig")
            .and_then(|p| p.as_array())
        {
            for entity in pii_entities {
                let entity_type = entity["type"].as_str().unwrap_or("");
                let action = entity["action"].as_str().unwrap_or("BLOCK");

                let pattern = match entity_type {
                    "EMAIL" => Some(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"),
                    "PHONE" => Some(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b"),
                    "US_SOCIAL_SECURITY_NUMBER" => Some(r"\b\d{3}-\d{2}-\d{4}\b"),
                    "CREDIT_DEBIT_CARD_NUMBER" => {
                        Some(r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b")
                    }
                    "IP_ADDRESS" => Some(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"),
                    _ => None,
                };

                if let Some(pat) = pattern {
                    if let Ok(re) = Regex::new(pat) {
                        for m in re.find_iter(text) {
                            assessments.push(json!({
                                "sensitiveInformationPolicy": {
                                    "piiEntities": [{
                                        "type": entity_type,
                                        "match": m.as_str(),
                                        "action": action
                                    }]
                                }
                            }));
                        }
                    }
                }
            }
        }

        // Regex patterns
        if let Some(regexes) = pii_policy.get("regexesConfig").and_then(|r| r.as_array()) {
            for regex_entry in regexes {
                let regex_name = regex_entry["name"].as_str().unwrap_or("");
                let pattern = regex_entry["pattern"].as_str().unwrap_or("");
                let action = regex_entry["action"].as_str().unwrap_or("BLOCK");

                if let Ok(re) = Regex::new(pattern) {
                    for m in re.find_iter(text) {
                        assessments.push(json!({
                            "sensitiveInformationPolicy": {
                                "regexes": [{
                                    "name": regex_name,
                                    "match": m.as_str(),
                                    "regex": pattern,
                                    "action": action
                                }]
                            }
                        }));
                    }
                }
            }
        }
    }

    assessments
}

// ── JSON helpers ───────────────────────────────────────────────────

/// Convert a stored guardrail policy (held in the `*Config` request shape) into
/// the shape Get/DescribeGuardrail returns. AWS drops the `Config` suffix from
/// the wrapper keys: `filtersConfig` -> `filters`, `topicsConfig` -> `topics`,
/// `piiEntitiesConfig` -> `piiEntities`, `regexesConfig` -> `regexes`,
/// `wordsConfig` -> `words`, `managedWordListsConfig` -> `managedWordLists`.
/// Echoing the request shape verbatim leaves the Terraform provider reading
/// empty `filters`/`topics`/... and re-planning the policy on every refresh.
fn policy_read_shape(config: &Value) -> Value {
    match config {
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                let key = k.strip_suffix("Config").unwrap_or(k).to_string();
                out.insert(key, policy_read_shape(v));
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(policy_read_shape).collect()),
        other => other.clone(),
    }
}

fn guardrail_to_json(g: &Guardrail) -> Value {
    let mut obj = json!({
        "guardrailId": g.guardrail_id,
        "guardrailArn": g.guardrail_arn,
        "name": g.name,
        "description": g.description,
        "status": g.status,
        "version": g.version,
        "blockedInputMessaging": g.blocked_input_messaging,
        "blockedOutputsMessaging": g.blocked_outputs_messaging,
        "createdAt": g.created_at.to_rfc3339(),
        "updatedAt": g.updated_at.to_rfc3339(),
    });

    if let Some(ref policy) = g.content_policy {
        obj["contentPolicy"] = policy_read_shape(policy);
    }
    if let Some(ref policy) = g.word_policy {
        obj["wordPolicy"] = policy_read_shape(policy);
    }
    if let Some(ref policy) = g.sensitive_information_policy {
        obj["sensitiveInformationPolicy"] = policy_read_shape(policy);
    }
    if let Some(ref policy) = g.topic_policy {
        obj["topicPolicy"] = policy_read_shape(policy);
    }
    if let Some(ref policy) = g.contextual_grounding_policy {
        obj["contextualGroundingPolicy"] = policy_read_shape(policy);
    }

    obj
}

fn guardrail_version_to_json(gv: &GuardrailVersion) -> Value {
    let mut obj = json!({
        "guardrailId": gv.guardrail_id,
        "guardrailArn": gv.guardrail_arn,
        "name": gv.name,
        "description": gv.description,
        "status": gv.status,
        "version": gv.version,
        "blockedInputMessaging": gv.blocked_input_messaging,
        "blockedOutputsMessaging": gv.blocked_outputs_messaging,
        "createdAt": gv.created_at.to_rfc3339(),
    });

    if let Some(ref policy) = gv.content_policy {
        obj["contentPolicy"] = policy_read_shape(policy);
    }
    if let Some(ref policy) = gv.word_policy {
        obj["wordPolicy"] = policy_read_shape(policy);
    }
    if let Some(ref policy) = gv.sensitive_information_policy {
        obj["sensitiveInformationPolicy"] = policy_read_shape(policy);
    }
    if let Some(ref policy) = gv.topic_policy {
        obj["topicPolicy"] = policy_read_shape(policy);
    }
    if let Some(ref policy) = gv.contextual_grounding_policy {
        obj["contextualGroundingPolicy"] = policy_read_shape(policy);
    }

    obj
}

/// Check if `word` appears in `text` at word boundaries (not as a substring of another word).
fn word_boundary_match(text: &str, word: &str) -> bool {
    let mut start = 0;
    while let Some(pos) = text[start..].find(word) {
        let abs_pos = start + pos;
        let before_ok = abs_pos == 0 || !text.as_bytes()[abs_pos - 1].is_ascii_alphanumeric();
        let after_pos = abs_pos + word.len();
        let after_ok =
            after_pos >= text.len() || !text.as_bytes()[after_pos].is_ascii_alphanumeric();
        if before_ok && after_ok {
            return true;
        }
        start = abs_pos + 1;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_aws::arn::Arn;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn empty_guardrail(id: &str) -> Guardrail {
        Guardrail {
            guardrail_id: id.to_string(),
            guardrail_arn: Arn::new("bedrock", "us-east-1", "123", &format!("guardrail/{id}"))
                .to_string(),
            name: id.to_string(),
            description: String::new(),
            status: "READY".to_string(),
            version: "DRAFT".to_string(),
            next_version_number: 1,
            blocked_input_messaging: "blocked input".to_string(),
            blocked_outputs_messaging: "blocked output".to_string(),
            content_policy: None,
            word_policy: None,
            sensitive_information_policy: None,
            topic_policy: None,
            contextual_grounding_policy: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn shared_state() -> SharedBedrockState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                "123456789012",
                "us-east-1",
                "http://localhost:4566",
            ),
        ))
    }

    #[test]
    fn word_boundary_matches_whole_word_only() {
        assert!(word_boundary_match("the damn cat", "damn"));
        assert!(word_boundary_match("damn!", "damn"));
        assert!(!word_boundary_match("damnation is long", "damn"));
        assert!(!word_boundary_match("subdamned", "damn"));
    }

    #[test]
    fn evaluate_content_custom_word_match() {
        let mut g = empty_guardrail("g1");
        g.word_policy = Some(json!({
            "wordsConfig": [{"text": "secret"}]
        }));
        let a = evaluate_content(&g, "this contains SECRET stuff");
        assert_eq!(a.len(), 1);
        assert_eq!(a[0]["wordPolicy"]["customWords"][0]["match"], "secret");
    }

    #[test]
    fn evaluate_content_profanity_managed_list() {
        let mut g = empty_guardrail("g1");
        g.word_policy = Some(json!({
            "managedWordListsConfig": [{"type": "PROFANITY"}]
        }));
        let a = evaluate_content(&g, "oh damn, that hurt");
        assert!(!a.is_empty());
        assert_eq!(
            a[0]["wordPolicy"]["managedWordLists"][0]["type"],
            "PROFANITY"
        );
    }

    #[test]
    fn evaluate_content_topic_match() {
        let mut g = empty_guardrail("g1");
        g.topic_policy = Some(json!({
            "topicsConfig": [{
                "name": "Politics",
                "examples": ["election results"],
            }]
        }));
        let a = evaluate_content(&g, "The election results were surprising");
        assert_eq!(a.len(), 1);
        assert_eq!(a[0]["topicPolicy"]["topics"][0]["name"], "Politics");
    }

    #[test]
    fn evaluate_content_pii_email_detected() {
        let mut g = empty_guardrail("g1");
        g.sensitive_information_policy = Some(json!({
            "piiEntitiesConfig": [{"type": "EMAIL", "action": "BLOCK"}]
        }));
        let a = evaluate_content(&g, "contact me at user@example.com please");
        assert_eq!(a.len(), 1);
        assert_eq!(
            a[0]["sensitiveInformationPolicy"]["piiEntities"][0]["match"],
            "user@example.com"
        );
    }

    #[test]
    fn evaluate_content_pii_phone_detected() {
        let mut g = empty_guardrail("g1");
        g.sensitive_information_policy = Some(json!({
            "piiEntitiesConfig": [{"type": "PHONE", "action": "BLOCK"}]
        }));
        let a = evaluate_content(&g, "call 555-123-4567 now");
        assert_eq!(a.len(), 1);
    }

    #[test]
    fn evaluate_content_pii_unknown_type_ignored() {
        let mut g = empty_guardrail("g1");
        g.sensitive_information_policy = Some(json!({
            "piiEntitiesConfig": [{"type": "BOGUS", "action": "BLOCK"}]
        }));
        let a = evaluate_content(&g, "test user@example.com");
        assert!(a.is_empty());
    }

    #[test]
    fn evaluate_content_regex_matches() {
        let mut g = empty_guardrail("g1");
        g.sensitive_information_policy = Some(json!({
            "regexesConfig": [{
                "name": "code",
                "pattern": r"CODE-\d+",
                "action": "ANONYMIZE"
            }]
        }));
        let a = evaluate_content(&g, "ref CODE-12345 here");
        assert_eq!(a.len(), 1);
        assert_eq!(
            a[0]["sensitiveInformationPolicy"]["regexes"][0]["match"],
            "CODE-12345"
        );
    }

    #[test]
    fn evaluate_content_regex_invalid_pattern_skipped() {
        let mut g = empty_guardrail("g1");
        g.sensitive_information_policy = Some(json!({
            "regexesConfig": [{
                "name": "bad",
                "pattern": "(unclosed",
                "action": "BLOCK"
            }]
        }));
        let a = evaluate_content(&g, "text");
        assert!(a.is_empty());
    }

    #[test]
    fn evaluate_content_empty_returns_no_assessments() {
        let g = empty_guardrail("g1");
        let a = evaluate_content(&g, "any text");
        assert!(a.is_empty());
    }

    fn make_req() -> AwsRequest {
        use bytes::Bytes;
        use http::Method;
        use std::collections::HashMap;
        AwsRequest {
            service: "bedrock".to_string(),
            action: "CreateGuardrail".to_string(),
            method: Method::POST,
            raw_path: "/guardrails".to_string(),
            raw_query: String::new(),
            path_segments: vec!["guardrails".to_string()],
            query_params: HashMap::new(),
            headers: http::HeaderMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "req-id".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn create_guardrail_requires_name() {
        let state = shared_state();
        let req = make_req();
        let body = json!({});
        let err = create_guardrail(&state, &req, &body).err().unwrap();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn create_guardrail_roundtrip() {
        let state = shared_state();
        let req = make_req();
        let body = json!({
            "name": "my-guard",
            "blockedInputMessaging": "NOPE-IN",
            "blockedOutputsMessaging": "NOPE-OUT",
            "description": "some guard",
            "wordPolicyConfig": {"wordsConfig": []},
        });
        let resp = create_guardrail(&state, &req, &body).unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);
        let accts = state.read();
        let empty = crate::state::BedrockState::new(&req.account_id, &req.region);
        let s = accts.get(&req.account_id).unwrap_or(&empty);
        assert_eq!(s.guardrails.len(), 1);
        let g = s.guardrails.values().next().unwrap();
        assert_eq!(g.name, "my-guard");
        assert_eq!(g.blocked_input_messaging, "NOPE-IN");
    }

    #[test]
    fn get_guardrail_not_found() {
        let state = shared_state();
        let req = make_req();
        let err = get_guardrail(&state, &req, "missing").err().unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn delete_guardrail_removes_entry() {
        let state = shared_state();
        let req = make_req();
        let gid = "gid-1";
        state
            .write()
            .default_mut()
            .guardrails
            .insert(gid.to_string(), empty_guardrail(gid));
        delete_guardrail(&state, &req, gid).unwrap();
        assert!(state.read().default_ref().guardrails.is_empty());
    }

    #[test]
    fn delete_guardrail_not_found() {
        let state = shared_state();
        let req = make_req();
        let err = delete_guardrail(&state, &req, "missing").err().unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn list_guardrails_returns_all() {
        let state = shared_state();
        state
            .write()
            .default_mut()
            .guardrails
            .insert("a".to_string(), empty_guardrail("a"));
        state
            .write()
            .default_mut()
            .guardrails
            .insert("b".to_string(), empty_guardrail("b"));
        let req = make_req();
        let resp = list_guardrails(&state, &req).unwrap();
        assert_eq!(resp.status, StatusCode::OK);
    }

    #[test]
    fn apply_guardrail_noop_with_no_policies() {
        let state = shared_state();
        let req = make_req();
        state
            .write()
            .default_mut()
            .guardrails
            .insert("g".to_string(), empty_guardrail("g"));
        let body = br#"{"content":[{"text":{"text":"hello"}}],"source":"INPUT"}"#;
        let resp = apply_guardrail(&state, &req, "g", "DRAFT", body).unwrap();
        assert_eq!(resp.status, StatusCode::OK);
    }

    #[test]
    fn apply_guardrail_intervenes_on_blocked_word() {
        let state = shared_state();
        let req = make_req();
        let mut g = empty_guardrail("g");
        g.word_policy = Some(json!({"wordsConfig": [{"text": "forbidden"}]}));
        state
            .write()
            .default_mut()
            .guardrails
            .insert("g".to_string(), g);
        let body = br#"{"content":[{"text":{"text":"this is forbidden"}}],"source":"INPUT"}"#;
        let resp = apply_guardrail(&state, &req, "g", "DRAFT", body).unwrap();
        let body_str = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body_str.contains("GUARDRAIL_INTERVENED"));
        assert!(body_str.contains("blocked input"));
    }

    #[test]
    fn apply_guardrail_output_source_uses_output_message() {
        let state = shared_state();
        let req = make_req();
        let mut g = empty_guardrail("g");
        g.word_policy = Some(json!({"wordsConfig": [{"text": "x"}]}));
        state
            .write()
            .default_mut()
            .guardrails
            .insert("g".to_string(), g);
        let body = br#"{"content":[{"text":{"text":"contains x value"}}],"source":"OUTPUT"}"#;
        let resp = apply_guardrail(&state, &req, "g", "DRAFT", body).unwrap();
        let body_str = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body_str.contains("blocked output"));
    }

    #[test]
    fn apply_guardrail_missing_returns_not_found() {
        let state = shared_state();
        let req = make_req();
        let body = br#"{"content":[],"source":"INPUT"}"#;
        let err = apply_guardrail(&state, &req, "missing", "DRAFT", body)
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn apply_guardrail_missing_version_returns_not_found() {
        let state = shared_state();
        let req = make_req();
        state
            .write()
            .default_mut()
            .guardrails
            .insert("g".to_string(), empty_guardrail("g"));
        let body = br#"{"content":[],"source":"INPUT"}"#;
        let err = apply_guardrail(&state, &req, "g", "99", body)
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    // ── additional coverage ────────────────────────────────────────────

    #[test]
    fn update_guardrail_changes_fields() {
        let state = shared_state();
        let req = make_req();
        state
            .write()
            .default_mut()
            .guardrails
            .insert("g1".to_string(), empty_guardrail("g1"));

        let body = json!({
            "name": "renamed",
            "description": "new desc",
            "blockedInputMessaging": "new-in",
            "blockedOutputsMessaging": "new-out",
            "wordPolicyConfig": {"wordsConfig": [{"text": "block-me"}]},
            "topicPolicyConfig": {"topicsConfig": []},
            "contentPolicyConfig": {"filtersConfig": []},
            "sensitiveInformationPolicyConfig": {"piiEntitiesConfig": []},
        });
        update_guardrail(&state, &req, "g1", &body).unwrap();

        let st = state.read();
        let g = st.default_ref().guardrails.get("g1").unwrap();
        assert_eq!(g.name, "renamed");
        assert_eq!(g.description, "new desc");
        assert_eq!(g.blocked_input_messaging, "new-in");
        assert_eq!(g.blocked_outputs_messaging, "new-out");
        assert!(g.word_policy.is_some());
        assert!(g.topic_policy.is_some());
        assert!(g.content_policy.is_some());
        assert!(g.sensitive_information_policy.is_some());
    }

    #[test]
    fn update_guardrail_not_found() {
        let state = shared_state();
        let req = make_req();
        let err = update_guardrail(&state, &req, "missing", &json!({}))
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn create_guardrail_version_increments_and_stores() {
        let state = shared_state();
        let req = make_req();
        state
            .write()
            .default_mut()
            .guardrails
            .insert("g1".to_string(), empty_guardrail("g1"));

        let resp = create_guardrail_version(&state, &req, "g1", &json!({"description": "v1-desc"}))
            .unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);
        let body: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(body["version"], "1");

        create_guardrail_version(&state, &req, "g1", &json!({})).unwrap();
        let st = state.read();
        assert_eq!(
            st.default_ref()
                .guardrail_versions
                .keys()
                .filter(|(id, _)| id == "g1")
                .count(),
            2
        );
        assert_eq!(
            st.default_ref()
                .guardrails
                .get("g1")
                .unwrap()
                .next_version_number,
            3
        );
    }

    #[test]
    fn create_guardrail_version_not_found() {
        let state = shared_state();
        let req = make_req();
        let err = create_guardrail_version(&state, &req, "missing", &json!({}))
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn get_guardrail_by_version_returns_version_json() {
        let state = shared_state();
        state
            .write()
            .default_mut()
            .guardrails
            .insert("g1".to_string(), empty_guardrail("g1"));
        let mut req = make_req();
        create_guardrail_version(&state, &req, "g1", &json!({})).unwrap();

        req.query_params
            .insert("guardrailVersion".to_string(), "1".to_string());
        let resp = get_guardrail(&state, &req, "g1").unwrap();
        assert_eq!(resp.status, StatusCode::OK);
    }

    #[test]
    fn get_guardrail_unknown_version_not_found() {
        let state = shared_state();
        state
            .write()
            .default_mut()
            .guardrails
            .insert("g1".to_string(), empty_guardrail("g1"));
        let mut req = make_req();
        req.query_params
            .insert("guardrailVersion".to_string(), "99".to_string());
        let err = get_guardrail(&state, &req, "g1").err().unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn delete_guardrail_removes_versions_too() {
        let state = shared_state();
        let req = make_req();
        state
            .write()
            .default_mut()
            .guardrails
            .insert("g1".to_string(), empty_guardrail("g1"));
        create_guardrail_version(&state, &req, "g1", &json!({})).unwrap();

        delete_guardrail(&state, &req, "g1").unwrap();
        let st = state.read();
        assert!(st.default_ref().guardrails.is_empty());
        assert!(st.default_ref().guardrail_versions.is_empty());
    }

    #[test]
    fn list_guardrails_paginates_by_id() {
        let state = shared_state();
        for i in 0..3 {
            let id = format!("g{i}");
            state
                .write()
                .default_mut()
                .guardrails
                .insert(id.clone(), empty_guardrail(&id));
        }
        let mut req = make_req();
        req.query_params
            .insert("maxResults".to_string(), "2".to_string());
        let resp = list_guardrails(&state, &req).unwrap();
        let body: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(body["guardrails"].as_array().unwrap().len(), 2);
        let token = body["nextToken"].as_str().unwrap().to_string();

        req.query_params.insert("nextToken".to_string(), token);
        let resp = list_guardrails(&state, &req).unwrap();
        let body: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(body["guardrails"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn apply_guardrail_passes_through_text_when_allowed() {
        let state = shared_state();
        let req = make_req();
        state
            .write()
            .default_mut()
            .guardrails
            .insert("g".to_string(), empty_guardrail("g"));
        let body = br#"{"content":[{"text":"hello"}, {"text":{"text":"world"}}],"source":"INPUT"}"#;
        let resp = apply_guardrail(&state, &req, "g", "DRAFT", body).unwrap();
        let body: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(body["action"], "NONE");
        assert_eq!(body["outputs"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn apply_guardrail_uses_version_when_numeric() {
        let state = shared_state();
        let req = make_req();
        let mut g = empty_guardrail("g");
        g.word_policy = Some(json!({"wordsConfig": [{"text": "nope"}]}));
        state
            .write()
            .default_mut()
            .guardrails
            .insert("g".to_string(), g);
        create_guardrail_version(&state, &req, "g", &json!({})).unwrap();

        let body = br#"{"content":[{"text":"say nope here"}],"source":"INPUT"}"#;
        let resp = apply_guardrail(&state, &req, "g", "1", body).unwrap();
        let body: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(body["action"], "GUARDRAIL_INTERVENED");
    }
}
