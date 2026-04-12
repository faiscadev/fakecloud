use chrono::Utc;
use http::StatusCode;
use regex::Regex;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{Guardrail, GuardrailVersion, SharedBedrockState};

pub fn create_guardrail(
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

    let guardrail_id = Uuid::new_v4().to_string()[..8].to_string();
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
        created_at: now,
        updated_at: now,
    };

    let mut s = state.write();
    s.guardrails.insert(guardrail_id.clone(), guardrail);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({
            "guardrailId": guardrail_id,
            "guardrailArn": guardrail_arn,
            "version": "DRAFT",
            "createdAt": now.to_rfc3339(),
        }))
        .unwrap(),
    ))
}

pub fn get_guardrail(
    state: &SharedBedrockState,
    req: &AwsRequest,
    guardrail_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    // Check if a specific version is requested
    let version = req.query_params.get("guardrailVersion");

    let s = state.read();

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

pub fn list_guardrails(
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

pub fn update_guardrail(
    state: &SharedBedrockState,
    guardrail_id: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
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

    guardrail.updated_at = Utc::now();

    let resp = json!({
        "guardrailId": guardrail.guardrail_id,
        "guardrailArn": guardrail.guardrail_arn,
        "version": guardrail.version,
        "updatedAt": guardrail.updated_at.to_rfc3339(),
    });

    Ok(AwsResponse::ok_json(resp))
}

pub fn delete_guardrail(
    state: &SharedBedrockState,
    guardrail_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
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

pub fn create_guardrail_version(
    state: &SharedBedrockState,
    guardrail_id: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
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
        created_at: now,
    };

    let key = (guardrail_id.to_string(), version_str.clone());
    s.guardrail_versions.insert(key, version);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({
            "guardrailId": guardrail_id,
            "version": version_str,
        }))
        .unwrap(),
    ))
}

/// Handle the ApplyGuardrail API — evaluate content against a guardrail.
pub fn apply_guardrail(
    state: &SharedBedrockState,
    guardrail_id: &str,
    guardrail_version: &str,
    body: &[u8],
) -> Result<AwsResponse, AwsServiceError> {
    let input: Value = serde_json::from_slice(body).unwrap_or_default();

    let s = state.read();

    // Build a temporary guardrail for evaluation from DRAFT or versioned
    let not_found_err = || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Guardrail {guardrail_id} version {guardrail_version} not found"),
        )
    };

    let temp_guardrail = if guardrail_version == "DRAFT" {
        let g = s.guardrails.get(guardrail_id).ok_or_else(not_found_err)?;
        Guardrail {
            guardrail_id: g.guardrail_id.clone(),
            guardrail_arn: g.guardrail_arn.clone(),
            name: g.name.clone(),
            description: g.description.clone(),
            status: g.status.clone(),
            version: g.version.clone(),
            next_version_number: g.next_version_number,
            blocked_input_messaging: g.blocked_input_messaging.clone(),
            blocked_outputs_messaging: g.blocked_outputs_messaging.clone(),
            content_policy: g.content_policy.clone(),
            word_policy: g.word_policy.clone(),
            sensitive_information_policy: g.sensitive_information_policy.clone(),
            topic_policy: g.topic_policy.clone(),
            created_at: g.created_at,
            updated_at: g.updated_at,
        }
    } else {
        let key = (guardrail_id.to_string(), guardrail_version.to_string());
        let gv = s.guardrail_versions.get(&key).ok_or_else(not_found_err)?;
        Guardrail {
            guardrail_id: gv.guardrail_id.clone(),
            guardrail_arn: gv.guardrail_arn.clone(),
            name: gv.name.clone(),
            description: gv.description.clone(),
            status: gv.status.clone(),
            version: gv.version.clone(),
            next_version_number: 0,
            blocked_input_messaging: gv.blocked_input_messaging.clone(),
            blocked_outputs_messaging: gv.blocked_outputs_messaging.clone(),
            content_policy: gv.content_policy.clone(),
            word_policy: gv.word_policy.clone(),
            sensitive_information_policy: gv.sensitive_information_policy.clone(),
            topic_policy: gv.topic_policy.clone(),
            created_at: gv.created_at,
            updated_at: gv.created_at,
        }
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

    let assessments = evaluate_content(&temp_guardrail, &all_text);
    let action = if assessments.is_empty() {
        "NONE"
    } else {
        "GUARDRAIL_INTERVENED"
    };

    let source = input["source"].as_str().unwrap_or("INPUT");
    let outputs = if action == "GUARDRAIL_INTERVENED" {
        let msg = if source == "INPUT" {
            &temp_guardrail.blocked_input_messaging
        } else {
            &temp_guardrail.blocked_outputs_messaging
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

/// Evaluate content against a guardrail's configured policies.
/// Returns a list of assessment results.
pub fn evaluate_content(guardrail: &Guardrail, text: &str) -> Vec<Value> {
    let mut assessments = Vec::new();

    // Word policy evaluation
    if let Some(ref word_policy) = guardrail.word_policy {
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
                    for word in &profanity_words {
                        if text.to_lowercase().contains(word) {
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
    if let Some(ref topic_policy) = guardrail.topic_policy {
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
    if let Some(ref pii_policy) = guardrail.sensitive_information_policy {
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
        obj["contentPolicy"] = policy.clone();
    }
    if let Some(ref policy) = g.word_policy {
        obj["wordPolicy"] = policy.clone();
    }
    if let Some(ref policy) = g.sensitive_information_policy {
        obj["sensitiveInformationPolicy"] = policy.clone();
    }
    if let Some(ref policy) = g.topic_policy {
        obj["topicPolicy"] = policy.clone();
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
        obj["contentPolicy"] = policy.clone();
    }
    if let Some(ref policy) = gv.word_policy {
        obj["wordPolicy"] = policy.clone();
    }
    if let Some(ref policy) = gv.sensitive_information_policy {
        obj["sensitiveInformationPolicy"] = policy.clone();
    }
    if let Some(ref policy) = gv.topic_policy {
        obj["topicPolicy"] = policy.clone();
    }

    obj
}
