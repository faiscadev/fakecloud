use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{AutomatedReasoningBuildWorkflow, SharedBedrockState};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn find_policy_arn(
    policies: &std::collections::HashMap<String, crate::state::AutomatedReasoningPolicy>,
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

fn require_policy_arn(
    policies: &std::collections::HashMap<String, crate::state::AutomatedReasoningPolicy>,
    identifier: &str,
) -> Result<String, AwsServiceError> {
    find_policy_arn(policies, identifier).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Automated reasoning policy {identifier} not found"),
        )
    })
}

fn workflow_to_json(w: &AutomatedReasoningBuildWorkflow) -> Value {
    json!({
        "buildWorkflowId": w.workflow_id,
        "policyArn": w.policy_arn,
        "workflowType": w.workflow_type,
        "status": w.status,
        "createdAt": w.created_at.to_rfc3339(),
        "updatedAt": w.updated_at.to_rfc3339(),
    })
}

// ---------------------------------------------------------------------------
// 1. StartBuildWorkflow
// ---------------------------------------------------------------------------

pub fn start_build_workflow(
    state: &SharedBedrockState,
    policy_identifier: &str,
    workflow_type: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
    let policy_arn = require_policy_arn(&s.automated_reasoning_policies, policy_identifier)?;

    let workflow_id = Uuid::new_v4().to_string();
    let now = Utc::now();
    let workflow = AutomatedReasoningBuildWorkflow {
        workflow_id: workflow_id.clone(),
        policy_arn: policy_arn.clone(),
        workflow_type: workflow_type.to_string(),
        status: "InProgress".to_string(),
        created_at: now,
        updated_at: now,
    };

    s.ar_build_workflows
        .insert((policy_arn, workflow_id.clone()), workflow);

    Ok(AwsResponse::json(
        StatusCode::CREATED,
        serde_json::to_string(&json!({ "buildWorkflowId": workflow_id })).unwrap(),
    ))
}

// ---------------------------------------------------------------------------
// 2. GetBuildWorkflow
// ---------------------------------------------------------------------------

pub fn get_build_workflow(
    state: &SharedBedrockState,
    policy_identifier: &str,
    workflow_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let policy_arn = require_policy_arn(&s.automated_reasoning_policies, policy_identifier)?;

    let workflow = s
        .ar_build_workflows
        .get(&(policy_arn, workflow_id.to_string()))
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Build workflow {workflow_id} not found"),
            )
        })?;

    Ok(AwsResponse::ok_json(workflow_to_json(workflow)))
}

// ---------------------------------------------------------------------------
// 3. ListBuildWorkflows
// ---------------------------------------------------------------------------

pub fn list_build_workflows(
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
    let policy_arn = require_policy_arn(&s.automated_reasoning_policies, policy_identifier)?;

    let mut items: Vec<&AutomatedReasoningBuildWorkflow> = s
        .ar_build_workflows
        .iter()
        .filter(|((arn, _), _)| *arn == policy_arn)
        .map(|(_, w)| w)
        .collect();
    items.sort_by(|a, b| a.workflow_id.cmp(&b.workflow_id));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|w| w.workflow_id.as_str() > token.as_str())
            .unwrap_or(items.len())
    } else {
        0
    };

    let page: Vec<Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|w| workflow_to_json(w))
        .collect();

    let mut resp = json!({ "buildWorkflowSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some(last) = items.get(end - 1) {
            resp["nextToken"] = json!(last.workflow_id);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

// ---------------------------------------------------------------------------
// 4. CancelBuildWorkflow
// ---------------------------------------------------------------------------

pub fn cancel_build_workflow(
    state: &SharedBedrockState,
    policy_identifier: &str,
    workflow_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
    let policy_arn = require_policy_arn(&s.automated_reasoning_policies, policy_identifier)?;

    let workflow = s
        .ar_build_workflows
        .get_mut(&(policy_arn, workflow_id.to_string()))
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Build workflow {workflow_id} not found"),
            )
        })?;

    workflow.status = "Cancelled".to_string();
    workflow.updated_at = Utc::now();

    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}

// ---------------------------------------------------------------------------
// 5. DeleteBuildWorkflow
// ---------------------------------------------------------------------------

pub fn delete_build_workflow(
    state: &SharedBedrockState,
    policy_identifier: &str,
    workflow_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
    let policy_arn = require_policy_arn(&s.automated_reasoning_policies, policy_identifier)?;

    let key = (policy_arn.clone(), workflow_id.to_string());
    if s.ar_build_workflows.remove(&key).is_none() {
        return Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Build workflow {workflow_id} not found"),
        ));
    }

    // Clean up associated test results and annotations
    s.ar_test_results
        .retain(|(pa, wid, _), _| !(*pa == policy_arn && *wid == workflow_id));
    s.ar_annotations
        .retain(|(pa, wid), _| !(*pa == policy_arn && *wid == workflow_id));

    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}

// ---------------------------------------------------------------------------
// 6. GetBuildWorkflowResultAssets
// ---------------------------------------------------------------------------

pub fn get_build_workflow_result_assets(
    state: &SharedBedrockState,
    policy_identifier: &str,
    workflow_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let policy_arn = require_policy_arn(&s.automated_reasoning_policies, policy_identifier)?;

    if !s
        .ar_build_workflows
        .contains_key(&(policy_arn, workflow_id.to_string()))
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Build workflow {workflow_id} not found"),
        ));
    }

    Ok(AwsResponse::ok_json(json!({ "assets": [] })))
}

// ---------------------------------------------------------------------------
// 7. StartTestWorkflow
// ---------------------------------------------------------------------------

pub fn start_test_workflow(
    state: &SharedBedrockState,
    policy_identifier: &str,
    workflow_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let policy_arn = require_policy_arn(&s.automated_reasoning_policies, policy_identifier)?;

    if !s
        .ar_build_workflows
        .contains_key(&(policy_arn, workflow_id.to_string()))
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Build workflow {workflow_id} not found"),
        ));
    }

    let test_workflow_id = Uuid::new_v4().to_string();

    Ok(AwsResponse::ok_json(
        json!({ "testWorkflowId": test_workflow_id }),
    ))
}

// ---------------------------------------------------------------------------
// 8. GetTestResult
// ---------------------------------------------------------------------------

pub fn get_test_result(
    state: &SharedBedrockState,
    policy_identifier: &str,
    workflow_id: &str,
    test_case_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let policy_arn = require_policy_arn(&s.automated_reasoning_policies, policy_identifier)?;

    if !s
        .ar_build_workflows
        .contains_key(&(policy_arn.clone(), workflow_id.to_string()))
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Build workflow {workflow_id} not found"),
        ));
    }

    let result = s
        .ar_test_results
        .get(&(
            policy_arn,
            workflow_id.to_string(),
            test_case_id.to_string(),
        ))
        .cloned()
        .unwrap_or_else(|| {
            json!({
                "testCaseId": test_case_id,
                "status": "NotRun",
            })
        });

    Ok(AwsResponse::ok_json(result))
}

// ---------------------------------------------------------------------------
// 9. ListTestResults
// ---------------------------------------------------------------------------

pub fn list_test_results(
    state: &SharedBedrockState,
    policy_identifier: &str,
    workflow_id: &str,
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
    let policy_arn = require_policy_arn(&s.automated_reasoning_policies, policy_identifier)?;

    if !s
        .ar_build_workflows
        .contains_key(&(policy_arn.clone(), workflow_id.to_string()))
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Build workflow {workflow_id} not found"),
        ));
    }

    let mut items: Vec<(&String, &Value)> = s
        .ar_test_results
        .iter()
        .filter(|((pa, wid, _), _)| *pa == policy_arn && *wid == workflow_id)
        .map(|((_, _, tcid), v)| (tcid, v))
        .collect();
    items.sort_by(|a, b| a.0.cmp(b.0));

    let start = if let Some(token) = next_token {
        items
            .iter()
            .position(|(tcid, _)| tcid.as_str() > token.as_str())
            .unwrap_or(items.len())
    } else {
        0
    };

    let page: Vec<&Value> = items
        .iter()
        .skip(start)
        .take(max_results)
        .map(|(_, v)| *v)
        .collect();

    let mut resp = json!({ "testResultSummaries": page });
    let end = start.saturating_add(max_results);
    if end < items.len() {
        if let Some((tcid, _)) = items.get(end - 1) {
            resp["nextToken"] = json!(tcid);
        }
    }

    Ok(AwsResponse::ok_json(resp))
}

// ---------------------------------------------------------------------------
// 10. GetAnnotations
// ---------------------------------------------------------------------------

pub fn get_annotations(
    state: &SharedBedrockState,
    policy_identifier: &str,
    workflow_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let policy_arn = require_policy_arn(&s.automated_reasoning_policies, policy_identifier)?;

    if !s
        .ar_build_workflows
        .contains_key(&(policy_arn.clone(), workflow_id.to_string()))
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Build workflow {workflow_id} not found"),
        ));
    }

    let annotations = s
        .ar_annotations
        .get(&(policy_arn, workflow_id.to_string()))
        .cloned()
        .unwrap_or_else(|| json!({ "annotations": [] }));

    Ok(AwsResponse::ok_json(annotations))
}

// ---------------------------------------------------------------------------
// 11. UpdateAnnotations
// ---------------------------------------------------------------------------

pub fn update_annotations(
    state: &SharedBedrockState,
    policy_identifier: &str,
    workflow_id: &str,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let mut s = state.write();
    let policy_arn = require_policy_arn(&s.automated_reasoning_policies, policy_identifier)?;

    if !s
        .ar_build_workflows
        .contains_key(&(policy_arn.clone(), workflow_id.to_string()))
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Build workflow {workflow_id} not found"),
        ));
    }

    s.ar_annotations
        .insert((policy_arn, workflow_id.to_string()), body.clone());

    Ok(AwsResponse::ok_json(body.clone()))
}

// ---------------------------------------------------------------------------
// 12. GetNextScenario
// ---------------------------------------------------------------------------

pub fn get_next_scenario(
    state: &SharedBedrockState,
    policy_identifier: &str,
    workflow_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let s = state.read();
    let policy_arn = require_policy_arn(&s.automated_reasoning_policies, policy_identifier)?;

    if !s
        .ar_build_workflows
        .contains_key(&(policy_arn, workflow_id.to_string()))
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Build workflow {workflow_id} not found"),
        ));
    }

    Ok(AwsResponse::ok_json(json!({
        "scenarioId": Uuid::new_v4().to_string(),
        "workflowId": workflow_id,
        "status": "Ready",
        "inputs": [],
    })))
}
