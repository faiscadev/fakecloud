use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{AutomatedReasoningPolicy, AutomatedReasoningTestCase, SharedBedrockState};

// ---------------------------------------------------------------------------
// Policy CRUD
// ---------------------------------------------------------------------------

pub fn create_automated_reasoning_policy(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let policy_name = body["policyName"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "policyName is required",
        )
    })?;

    let policy_id = Uuid::new_v4().to_string();
    let policy_arn = format!(
        "arn:aws:bedrock:{}:{}:automated-reasoning-policy/{}",
        req.region, req.account_id, policy_id
    );

    let now = Utc::now();
    let policy = AutomatedReasoningPolicy {
        policy_arn: policy_arn.clone(),
        policy_name: policy_name.to_string(),
        description: body["description"].as_str().map(String::from),
        policy_document: body.get("policyDocument").cloned().unwrap_or(json!({})),
        status: "ACTIVE".to_string(),
        version: "1".to_string(),
        versions: vec!["1".to_string()],
        created_at: now,
        updated_at: now,
    };

    let mut s = state.write();
    s.automated_reasoning_policies
        .insert(policy_arn.clone(), policy);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "policyArn": policy_arn })).unwrap(),
    ))
}

pub fn get_automated_reasoning_policy(
    state: &SharedBedrockState,
    identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let policy = find_policy(&s.automated_reasoning_policies, identifier).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Automated reasoning policy {identifier} not found"),
        )
    })?;

    Ok(AwsResponse::ok_json(policy_to_json(policy)))
}

pub fn list_automated_reasoning_policies(
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
    let mut items: Vec<&AutomatedReasoningPolicy> =
        s.automated_reasoning_policies.values().collect();
    items.sort_by(|a, b| a.policy_arn.cmp(&b.policy_arn));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|p| p.policy_arn.as_str() > token.as_str())
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
                "policyArn": p.policy_arn,
                "policyName": p.policy_name,
                "description": p.description,
                "status": p.status,
                "version": p.version,
                "createdAt": p.created_at.to_rfc3339(),
                "updatedAt": p.updated_at.to_rfc3339(),
            })
        })
        .collect();

    let mut resp = json!({ "policySummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.policy_arn);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn update_automated_reasoning_policy(
    state: &SharedBedrockState,
    identifier: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let key = find_policy_key(&s.automated_reasoning_policies, identifier).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Automated reasoning policy {identifier} not found"),
        )
    })?;

    let policy = s.automated_reasoning_policies.get_mut(&key).unwrap();

    if let Some(name) = body["policyName"].as_str() {
        policy.policy_name = name.to_string();
    }
    if let Some(desc) = body.get("description") {
        policy.description = desc.as_str().map(String::from);
    }
    if let Some(doc) = body.get("policyDocument") {
        policy.policy_document = doc.clone();
    }
    policy.updated_at = Utc::now();

    Ok(AwsResponse::ok_json(policy_to_json(policy)))
}

pub fn delete_automated_reasoning_policy(
    state: &SharedBedrockState,
    identifier: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let key = find_policy_key(&s.automated_reasoning_policies, identifier).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Automated reasoning policy {identifier} not found"),
        )
    })?;

    // Remove associated test cases
    let policy_arn = key.clone();
    s.automated_reasoning_test_cases
        .retain(|(arn, _), _| *arn != policy_arn);

    s.automated_reasoning_policies.remove(&key);
    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}

// ---------------------------------------------------------------------------
// Policy versions
// ---------------------------------------------------------------------------

pub fn create_automated_reasoning_policy_version(
    state: &SharedBedrockState,
    identifier: &str,
    _body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let key = find_policy_key(&s.automated_reasoning_policies, identifier).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Automated reasoning policy {identifier} not found"),
        )
    })?;

    let policy = s.automated_reasoning_policies.get_mut(&key).unwrap();

    let current: u32 = policy.version.parse().unwrap_or(1);
    let next = current.saturating_add(1);
    let version_str = next.to_string();
    policy.version = version_str.clone();
    policy.versions.push(version_str.clone());
    policy.updated_at = Utc::now();

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({
            "policyArn": policy.policy_arn,
            "version": version_str,
        }))
        .unwrap(),
    ))
}

pub fn export_automated_reasoning_policy_version(
    state: &SharedBedrockState,
    identifier: &str,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let policy = find_policy(&s.automated_reasoning_policies, identifier).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Automated reasoning policy {identifier} not found"),
        )
    })?;

    let requested_version = req.query_params.get("policyVersion");
    if let Some(ver) = requested_version {
        if !policy.versions.contains(ver) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Version {ver} not found for policy {identifier}"),
            ));
        }
    }

    Ok(AwsResponse::ok_json(json!({
        "policyArn": policy.policy_arn,
        "policyDocument": policy.policy_document,
        "version": requested_version.unwrap_or(&policy.version),
    })))
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

pub fn create_automated_reasoning_policy_test_case(
    state: &SharedBedrockState,
    policy_identifier: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let test_case_name = body["testCaseName"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "testCaseName is required",
        )
    })?;

    let mut s = state.write();

    let policy_arn = find_policy_key(&s.automated_reasoning_policies, policy_identifier)
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Automated reasoning policy {policy_identifier} not found"),
            )
        })?;

    let test_case_id = Uuid::new_v4().to_string();
    let now = Utc::now();
    let tc = AutomatedReasoningTestCase {
        test_case_id: test_case_id.clone(),
        policy_arn: policy_arn.clone(),
        test_case_name: test_case_name.to_string(),
        description: body["description"].as_str().map(String::from),
        input: body.get("input").cloned().unwrap_or(json!({})),
        expected_output: body.get("expectedOutput").cloned().unwrap_or(json!({})),
        created_at: now,
        updated_at: now,
    };

    s.automated_reasoning_test_cases
        .insert((policy_arn, test_case_id.clone()), tc);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "testCaseId": test_case_id })).unwrap(),
    ))
}

pub fn get_automated_reasoning_policy_test_case(
    state: &SharedBedrockState,
    policy_identifier: &str,
    test_case_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();

    let policy_arn = find_policy_key(&s.automated_reasoning_policies, policy_identifier)
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Automated reasoning policy {policy_identifier} not found"),
            )
        })?;

    let tc = s
        .automated_reasoning_test_cases
        .get(&(policy_arn, test_case_id.to_string()))
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Test case {test_case_id} not found"),
            )
        })?;

    Ok(AwsResponse::ok_json(test_case_to_json(tc)))
}

pub fn list_automated_reasoning_policy_test_cases(
    state: &SharedBedrockState,
    policy_identifier: &str,
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

    let policy_arn = find_policy_key(&s.automated_reasoning_policies, policy_identifier)
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Automated reasoning policy {policy_identifier} not found"),
            )
        })?;

    let mut items: Vec<&AutomatedReasoningTestCase> = s
        .automated_reasoning_test_cases
        .iter()
        .filter(|((arn, _), _)| *arn == policy_arn)
        .map(|(_, tc)| tc)
        .collect();
    items.sort_by(|a, b| a.test_case_id.cmp(&b.test_case_id));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|tc| tc.test_case_id.as_str() > token.as_str())
            .unwrap_or(items.len())
    } else {
        0
    };

    let page: Vec<Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|tc| {
            json!({
                "testCaseId": tc.test_case_id,
                "testCaseName": tc.test_case_name,
                "description": tc.description,
                "createdAt": tc.created_at.to_rfc3339(),
                "updatedAt": tc.updated_at.to_rfc3339(),
            })
        })
        .collect();

    let mut resp = json!({ "testCaseSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.test_case_id);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

pub fn update_automated_reasoning_policy_test_case(
    state: &SharedBedrockState,
    policy_identifier: &str,
    test_case_id: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let policy_arn = find_policy_key(&s.automated_reasoning_policies, policy_identifier)
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Automated reasoning policy {policy_identifier} not found"),
            )
        })?;

    let tc = s
        .automated_reasoning_test_cases
        .get_mut(&(policy_arn, test_case_id.to_string()))
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Test case {test_case_id} not found"),
            )
        })?;

    if let Some(name) = body["testCaseName"].as_str() {
        tc.test_case_name = name.to_string();
    }
    if let Some(desc) = body.get("description") {
        tc.description = desc.as_str().map(String::from);
    }
    if let Some(input) = body.get("input") {
        tc.input = input.clone();
    }
    if let Some(output) = body.get("expectedOutput") {
        tc.expected_output = output.clone();
    }
    tc.updated_at = Utc::now();

    Ok(AwsResponse::ok_json(test_case_to_json(tc)))
}

pub fn delete_automated_reasoning_policy_test_case(
    state: &SharedBedrockState,
    policy_identifier: &str,
    test_case_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();

    let policy_arn = find_policy_key(&s.automated_reasoning_policies, policy_identifier)
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Automated reasoning policy {policy_identifier} not found"),
            )
        })?;

    let key = (policy_arn, test_case_id.to_string());
    if s.automated_reasoning_test_cases.remove(&key).is_none() {
        return Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Test case {test_case_id} not found"),
        ));
    }

    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn find_policy<'a>(
    policies: &'a std::collections::HashMap<String, AutomatedReasoningPolicy>,
    identifier: &str,
) -> Option<&'a AutomatedReasoningPolicy> {
    policies.get(identifier).or_else(|| {
        policies.values().find(|p| {
            p.policy_name == identifier || p.policy_arn.ends_with(&format!("/{identifier}"))
        })
    })
}

fn find_policy_key(
    policies: &std::collections::HashMap<String, AutomatedReasoningPolicy>,
    identifier: &str,
) -> Option<String> {
    policies
        .iter()
        .find(|(k, p)| {
            *k == identifier
                || p.policy_name == identifier
                || p.policy_arn.ends_with(&format!("/{identifier}"))
        })
        .map(|(k, _)| k.clone())
}

fn policy_to_json(p: &AutomatedReasoningPolicy) -> Value {
    json!({
        "policyArn": p.policy_arn,
        "policyName": p.policy_name,
        "description": p.description,
        "policyDocument": p.policy_document,
        "status": p.status,
        "version": p.version,
        "versions": p.versions,
        "createdAt": p.created_at.to_rfc3339(),
        "updatedAt": p.updated_at.to_rfc3339(),
    })
}

fn test_case_to_json(tc: &AutomatedReasoningTestCase) -> Value {
    json!({
        "testCaseId": tc.test_case_id,
        "policyArn": tc.policy_arn,
        "testCaseName": tc.test_case_name,
        "description": tc.description,
        "input": tc.input,
        "expectedOutput": tc.expected_output,
        "createdAt": tc.created_at.to_rfc3339(),
        "updatedAt": tc.updated_at.to_rfc3339(),
    })
}
