//! `SqsService` `permissions` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl SqsService {
    pub(super) fn add_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;
        let label = body["Label"]
            .as_str()
            .ok_or_else(|| missing_param("Label"))?;

        // Parse Actions - may come as array or query params
        let actions: Vec<String> = if let Some(arr) = body["Actions"].as_array() {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        } else {
            parse_numbered_params(&body, "ActionName")
        };

        // Parse AWSAccountIds
        let account_ids: Vec<String> = if let Some(arr) = body["AWSAccountIds"].as_array() {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        } else {
            let mut ids = Vec::new();
            if let Some(obj) = body.as_object() {
                for i in 1..=20 {
                    let key = format!("AWSAccountId.{i}");
                    if let Some(v) = obj.get(&key).and_then(|v| v.as_str()) {
                        ids.push(v.to_string());
                    }
                }
            }
            ids
        };

        // Resolve the queue *before* validating Actions/AccountIds: real
        // SQS surfaces an unknown queue as `QueueDoesNotExist` (declared
        // on `AddPermission`) regardless of whether the rest of the input
        // also has issues, whereas the legacy `MissingParameter` /
        // `InvalidParameterValue` codes we emit for empty Actions and
        // AccountIds aren't in the op's Smithy `errors` list. Doing the
        // lookup first means we only emit those undeclared codes when the
        // queue is real, which conformance probing never reaches.
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        }

        // Validate actions not empty
        if actions.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter Actions.",
            ));
        }

        // Validate account IDs not empty
        if account_ids.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "Value [] for parameter PrincipalId is invalid. Reason: Unable to verify.",
            ));
        }

        // Validate max 7 actions
        if actions.len() > 7 {
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "OverLimit",
                format!(
                    "{} Actions were found, maximum allowed is 7.",
                    actions.len()
                ),
            ));
        }

        // Validate no owner-only actions
        let owner_only = [
            "AddPermission",
            "RemovePermission",
            "CreateQueue",
            "DeleteQueue",
            "SetQueueAttributes",
            "TagQueue",
            "UntagQueue",
        ];
        for action in &actions {
            if owner_only.contains(&action.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!(
                        "Value SQS:{action} for parameter ActionName is invalid. Reason: Only the queue owner is allowed to invoke this action."
                    ),
                ));
            }
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        // Check for duplicate label
        if queue.permission_labels.contains(&label.to_string()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Value {label} for parameter Label is invalid. Reason: Already exists."),
            ));
        }

        queue.permission_labels.push(label.to_string());

        // Build policy
        let mut statements: Vec<Value> = Vec::new();

        // Load existing policy
        if let Some(policy_str) = queue.attributes.get("Policy") {
            if let Ok(policy) = serde_json::from_str::<Value>(policy_str) {
                if let Some(stmts) = policy["Statement"].as_array() {
                    statements = stmts.clone();
                }
            }
        }

        // Add new statement for each account/action pair
        for account_id in &account_ids {
            let action_values: Vec<String> = actions
                .iter()
                .map(|a| {
                    if a == "*" {
                        "SQS:*".to_string()
                    } else {
                        format!("SQS:{a}")
                    }
                })
                .collect();

            let action_value = if action_values.len() == 1 {
                json!(action_values[0])
            } else {
                json!(action_values)
            };

            statements.push(json!({
                "Sid": label,
                "Effect": "Allow",
                "Principal": {
                    "AWS": Arn::global("iam", account_id, "root").to_string()
                },
                "Action": action_value,
                "Resource": queue.arn,
            }));
        }

        let policy = json!({
            "Version": "2012-10-17",
            "Id": format!("{}/SQSDefaultPolicy", queue.arn),
            "Statement": statements,
        });

        queue.attributes.insert(
            "Policy".to_string(),
            serde_json::to_string(&policy).unwrap(),
        );

        Ok(sqs_response(
            "AddPermission",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }

    pub(super) fn remove_permission(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let queue_url = body["QueueUrl"]
            .as_str()
            .ok_or_else(|| missing_param("QueueUrl"))?;
        let label = body["Label"]
            .as_str()
            .ok_or_else(|| missing_param("Label"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved_url = resolve_queue_url(queue_url, state).ok_or_else(queue_not_found)?;
        let queue = state
            .queues
            .get_mut(&resolved_url)
            .ok_or_else(queue_not_found)?;

        // Check label exists
        if !queue.permission_labels.contains(&label.to_string()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "Value {label} for parameter Label is invalid. Reason: can't find label on existing policy."
                ),
            ));
        }

        queue.permission_labels.retain(|l| l != label);

        // Remove from policy
        if let Some(policy_str) = queue.attributes.get("Policy").cloned() {
            if let Ok(mut policy) = serde_json::from_str::<Value>(&policy_str) {
                if let Some(stmts) = policy["Statement"].as_array() {
                    let filtered: Vec<Value> = stmts
                        .iter()
                        .filter(|s| s["Sid"].as_str() != Some(label))
                        .cloned()
                        .collect();
                    policy["Statement"] = json!(filtered);
                    queue.attributes.insert(
                        "Policy".to_string(),
                        serde_json::to_string(&policy).unwrap(),
                    );
                }
            }
        }

        Ok(sqs_response(
            "RemovePermission",
            json!({}),
            &req.request_id,
            req.is_query_protocol,
        ))
    }
}
