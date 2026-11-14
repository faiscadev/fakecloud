use http::StatusCode;
use serde_json::{json, Value};

use crate::validation::*;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::LogsService;
use chrono::Utc;

use super::extract_log_group_from_arn;
use crate::retention;
use crate::state::{AccountPolicy, DataProtectionPolicy, IndexPolicy, ResourcePolicy, Transformer};
use crate::transformer;

/// The model's `PolicyType` enum.
const POLICY_TYPES: &[&str] = &[
    "DATA_PROTECTION_POLICY",
    "SUBSCRIPTION_FILTER_POLICY",
    "FIELD_INDEX_POLICY",
    "TRANSFORMER_POLICY",
    "METRIC_EXTRACTION_POLICY",
    "RETENTION_POLICY",
];

fn invalid_parameter(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterException",
        message.into(),
    )
}

/// Read `dryRun` (default `false`). It is valid only for retention policies.
fn parse_dry_run(body: &Value, policy_type: &str) -> Result<bool, AwsServiceError> {
    let dry_run = match &body["dryRun"] {
        Value::Null => false,
        Value::Bool(b) => *b,
        _ => {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "SerializationException",
                "Value for 'dryRun' must be a boolean",
            ))
        }
    };
    if dry_run && policy_type != retention::RETENTION_POLICY {
        return Err(invalid_parameter(
            "dryRun is valid only when policyType is RETENTION_POLICY.",
        ));
    }
    Ok(dry_run)
}

impl LogsService {
    // ---- Resource Policies ----

    pub(crate) fn put_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_name = body["policyName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "policyName is required",
                )
            })?
            .to_string();
        let policy_document = body["policyDocument"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "policyDocument is required",
                )
            })?
            .to_string();

        let now = Utc::now().timestamp_millis();

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);

        // Check limit (10 per region) only if adding new
        if !state.resource_policies.contains_key(&policy_name)
            && state.resource_policies.len() >= 10
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "LimitExceededException",
                "Resource limit exceeded.",
            ));
        }

        let policy = ResourcePolicy {
            policy_name: policy_name.clone(),
            policy_document: policy_document.clone(),
            last_updated_time: now,
        };

        state.resource_policies.insert(policy_name.clone(), policy);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "resourcePolicy": {
                    "policyName": policy_name,
                    "policyDocument": policy_document,
                    "lastUpdatedTime": now,
                }
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn describe_resource_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "policyScope",
            &body["policyScope"],
            &["ACCOUNT", "RESOURCE"],
        )?;
        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);

        let mut policies: Vec<Value> = state
            .resource_policies
            .values()
            .map(|p| {
                json!({
                    "policyName": p.policy_name,
                    "policyDocument": p.policy_document,
                    "lastUpdatedTime": p.last_updated_time,
                })
            })
            .collect();
        policies.sort_by(|a, b| {
            a["policyName"]
                .as_str()
                .unwrap()
                .cmp(b["policyName"].as_str().unwrap())
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "resourcePolicies": policies })).unwrap(),
        ))
    }

    pub(crate) fn delete_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_name = body["policyName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyName is required",
            )
        })?;

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        if state.resource_policies.remove(policy_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Policy with name [{policy_name}] does not exist"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Account Policies ----

    pub(crate) fn put_account_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_enum_value("policyType", &body["policyType"], POLICY_TYPES)?;
        validate_optional_enum_value("scope", &body["scope"], &["ALL"])?;
        let policy_name = body["policyName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyName is required",
            )
        })?;
        let policy_type = body["policyType"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyType is required",
            )
        })?;
        let policy_document = body["policyDocument"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyDocument is required",
            )
        })?;

        let dry_run = parse_dry_run(&body, policy_type)?;
        let selection_criteria = body["selectionCriteria"].as_str().map(|s| s.to_string());
        if policy_type == retention::RETENTION_POLICY {
            retention::parse_document(policy_document).map_err(invalid_parameter)?;
            retention::parse_criteria(selection_criteria.as_deref()).map_err(invalid_parameter)?;
        }

        let now = Utc::now().timestamp_millis();
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let account_id = state.account_id.clone();
        let scope = body["scope"].as_str().map(|s| s.to_string());
        let key = (policy_name.to_string(), policy_type.to_string());
        let existing = state.account_policies.get(&key);

        if policy_type == retention::RETENTION_POLICY
            && existing.is_none()
            && state
                .account_policies
                .values()
                .filter(|p| p.policy_type == retention::RETENTION_POLICY)
                .count()
                >= retention::MAX_RETENTION_POLICIES
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "LimitExceededException",
                format!(
                    "An account can have at most {} retention policies in a Region.",
                    retention::MAX_RETENTION_POLICIES
                ),
            ));
        }

        let policy = AccountPolicy {
            policy_name: policy_name.to_string(),
            policy_type: policy_type.to_string(),
            policy_document: policy_document.to_string(),
            scope: scope.clone(),
            selection_criteria: selection_criteria.clone(),
            account_id: account_id.clone(),
            last_updated_time: now,
            created_at: existing.map_or(now, AccountPolicy::created_time),
        };

        if dry_run {
            // Preview only: nothing is saved.
            let before = retention::parsed_policies(&state.account_policies);
            let replaced = existing.and_then(retention::ParsedPolicy::from_stored);
            let added = retention::ParsedPolicy::from_stored(&policy)
                .expect("retention policy validated above");
            let mut after: Vec<_> = before
                .iter()
                .filter(|p| p.name != policy_name)
                .cloned()
                .collect();
            after.push(added.clone());
            let result = retention::dry_run_result(
                state.log_groups.values(),
                |g| added.applies_to(g) || replaced.as_ref().is_some_and(|r| r.applies_to(g)),
                &before,
                &after,
            );
            return Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({ "dryRunResult": result })).unwrap(),
            ));
        }

        state.account_policies.insert(key, policy);

        let mut result = json!({
            "accountPolicy": {
                "policyName": policy_name,
                "policyType": policy_type,
                "policyDocument": policy_document,
                "accountId": account_id,
                "lastUpdatedTime": now,
            }
        });
        if let Some(s) = scope {
            result["accountPolicy"]["scope"] = json!(s);
        }
        if let Some(s) = selection_criteria {
            result["accountPolicy"]["selectionCriteria"] = json!(s);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    pub(crate) fn describe_account_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_enum_value("policyType", &body["policyType"], POLICY_TYPES)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        let policy_type = body["policyType"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyType is required",
            )
        })?;
        let policy_name = body["policyName"].as_str();

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let policies: Vec<Value> = state
            .account_policies
            .values()
            .filter(|p| {
                p.policy_type == policy_type && policy_name.is_none_or(|n| p.policy_name == n)
            })
            .map(|p| {
                let mut obj = json!({
                    "policyName": p.policy_name,
                    "policyType": p.policy_type,
                    "policyDocument": p.policy_document,
                    "accountId": p.account_id,
                    "lastUpdatedTime": p.last_updated_time,
                });
                if let Some(ref s) = p.scope {
                    obj["scope"] = json!(s);
                }
                if let Some(ref s) = p.selection_criteria {
                    obj["selectionCriteria"] = json!(s);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "accountPolicies": policies })).unwrap(),
        ))
    }

    pub(crate) fn delete_account_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_name = body["policyName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyName is required",
            )
        })?;
        let policy_type = body["policyType"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyType is required",
            )
        })?;

        validate_optional_enum_value("policyType", &body["policyType"], POLICY_TYPES)?;
        let dry_run = parse_dry_run(&body, policy_type)?;

        let key = (policy_name.to_string(), policy_type.to_string());
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let Some(existing) = state.account_policies.get(&key) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Account policy {policy_name} of type {policy_type} not found"),
            ));
        };

        if dry_run {
            // Preview only: the policy stays in place.
            let before = retention::parsed_policies(&state.account_policies);
            let removed = retention::ParsedPolicy::from_stored(existing);
            let after: Vec<_> = before
                .iter()
                .filter(|p| p.name != policy_name)
                .cloned()
                .collect();
            let result = retention::dry_run_result(
                state.log_groups.values(),
                |g| removed.as_ref().is_some_and(|r| r.applies_to(g)),
                &before,
                &after,
            );
            return Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({ "dryRunResult": result })).unwrap(),
            ));
        }

        state.account_policies.remove(&key);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Data Protection Policies ----

    pub(crate) fn put_data_protection_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();
        let policy_document = body["policyDocument"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "policyDocument is required",
                )
            })?
            .to_string();

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id.clone()
        };

        let now = Utc::now().timestamp_millis();
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;
        // Echo back the identifier exactly as supplied (AWS round-trips it), so
        // the resource's `log_group_name` matches the configured value instead of
        // drifting to the ARN form.
        let log_group_id_resp = log_group_id.clone();

        group.data_protection_policy = Some(DataProtectionPolicy {
            policy_document: policy_document.clone(),
            last_updated_time: now,
        });
        group.data_protection_deleted = false;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "logGroupIdentifier": log_group_id_resp,
                "policyDocument": policy_document,
                "lastUpdatedTime": now,
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn get_data_protection_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id.clone()
        };

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let group = state.log_groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        let mut result = json!({
            "logGroupIdentifier": log_group_id,
        });
        if let Some(ref dp) = group.data_protection_policy {
            result["policyDocument"] = json!(dp.policy_document);
            result["lastUpdatedTime"] = json!(dp.last_updated_time);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    pub(crate) fn delete_data_protection_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id
        };

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        if group.data_protection_policy.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "No data protection policy found for this log group",
            ));
        }

        group.data_protection_policy = None;
        group.data_protection_deleted = true;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Index Policies ----

    pub(crate) fn put_index_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();
        let policy_document = body["policyDocument"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "policyDocument is required",
                )
            })?
            .to_string();

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id.clone()
        };

        let policy_name = body["policyName"].as_str().unwrap_or("default").to_string();

        let now = Utc::now().timestamp_millis();
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        // Replace existing policy with same name, or add new one
        if let Some(existing) = group
            .index_policies
            .iter_mut()
            .find(|p| p.policy_name == policy_name)
        {
            existing.policy_document = policy_document.clone();
            existing.last_updated_time = now;
        } else {
            group.index_policies.push(IndexPolicy {
                policy_name: policy_name.clone(),
                policy_document: policy_document.clone(),
                last_updated_time: now,
            });
        }

        // The log-group ARN stored on the group keeps the `:*` resource-suffix;
        // the index-policy `logGroupIdentifier` reports it without the suffix so
        // the resource's `log_group_name` round-trips (otherwise the read sees a
        // `:*`-suffixed identifier and forces replacement).
        let log_group_identifier = group.log_group_arn().to_string();
        let result = json!({
            "indexPolicy": {
                "policyName": policy_name,
                "policyDocument": policy_document,
                "logGroupIdentifier": log_group_identifier,
                "lastUpdateTime": now,
            }
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    pub(crate) fn describe_index_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        let log_group_ids = body["logGroupIdentifiers"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupIdentifiers is required",
            )
        })?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let mut policies = Vec::new();

        for id_val in log_group_ids {
            let id = id_val.as_str().unwrap_or("");
            let group_name = if id.starts_with("arn:") {
                extract_log_group_from_arn(id).unwrap_or_default()
            } else {
                id.to_string()
            };
            if let Some(group) = state.log_groups.get(&group_name) {
                let log_group_identifier = group.log_group_arn();
                for p in &group.index_policies {
                    policies.push(json!({
                        "policyName": p.policy_name,
                        "policyDocument": p.policy_document,
                        "logGroupIdentifier": log_group_identifier,
                        "lastUpdateTime": p.last_updated_time,
                    }));
                }
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "indexPolicies": policies })).unwrap(),
        ))
    }

    pub(crate) fn delete_index_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id
        };

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        if group.index_policies.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "No index policy found for this log group",
            ));
        }

        group.index_policies.clear();
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn describe_field_indexes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        let log_group_ids = body["logGroupIdentifiers"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupIdentifiers is required",
            )
        })?;
        // `indexCategories` filters the result set. Every index fakecloud
        // reports comes from a customer-created index policy, so they are all
        // `CUSTOM`; asking for only the other categories returns nothing.
        let categories = match body["indexCategories"].as_array() {
            Some(list) => {
                if list.len() > 4 {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterException",
                        "indexCategories may contain at most 4 values",
                    ));
                }
                let mut wanted = Vec::with_capacity(list.len());
                for v in list {
                    let c = v.as_str().unwrap_or_default();
                    if !matches!(c, "DEFAULT" | "CUSTOM" | "AUTO" | "INACTIVE") {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterException",
                            format!("indexCategories contains an unsupported value: {c}"),
                        ));
                    }
                    wanted.push(c.to_string());
                }
                Some(wanted)
            }
            None => None,
        };
        let want_custom = categories.is_none_or(|c| c.iter().any(|c| c == "CUSTOM"));

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let mut field_indexes = Vec::new();

        for id_val in log_group_ids {
            if !want_custom {
                break;
            }
            let id = id_val.as_str().unwrap_or("");
            let group_name = if id.starts_with("arn:") {
                extract_log_group_from_arn(id).unwrap_or_default()
            } else {
                id.to_string()
            };
            let Some(group) = state.log_groups.get(&group_name) else {
                continue;
            };
            for p in &group.index_policies {
                let parsed: Value = serde_json::from_str(&p.policy_document).unwrap_or(Value::Null);
                if let Some(fields) = parsed.get("Fields").and_then(|v| v.as_array()) {
                    for f in fields {
                        if let Some(name) = f.as_str() {
                            field_indexes.push(json!({
                                "logGroupIdentifier": group.log_group_arn(),
                                "fieldIndexName": name,
                                "lastScanTime": p.last_updated_time,
                                "firstEventTime": p.last_updated_time,
                                "lastEventTime": p.last_updated_time,
                                "type": "FIELD_INDEX",
                                "indexCategory": "CUSTOM",
                            }));
                        }
                    }
                }
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "fieldIndexes": field_indexes })).unwrap(),
        ))
    }

    // ---- Transformers ----

    pub(crate) fn put_transformer(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();
        let transformer_config = body.get("transformerConfig").cloned().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "transformerConfig is required",
            )
        })?;

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id.clone()
        };

        let now = Utc::now().timestamp_millis();
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        group.transformer = Some(Transformer {
            transformer_config,
            creation_time: now,
            last_modified_time: now,
        });

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn get_transformer(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id.clone()
        };

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let group = state.log_groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        let mut result = json!({
            "logGroupIdentifier": group.log_group_arn(),
        });
        if let Some(ref t) = group.transformer {
            result["transformerConfig"] = t.transformer_config.clone();
            result["creationTime"] = json!(t.creation_time);
            result["lastModifiedTime"] = json!(t.last_modified_time);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    pub(crate) fn delete_transformer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let log_group_id = body["logGroupIdentifier"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifier is required",
                )
            })?
            .to_string();

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(&log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id
        };

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        group.transformer = None;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn test_transformer(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let transformer_config = body.get("transformerConfig").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "transformerConfig is required",
            )
        })?;
        let log_event_messages = body["logEventMessages"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logEventMessages is required",
            )
        })?;

        let transformed: Vec<Value> = log_event_messages
            .iter()
            .map(|msg| {
                let message = msg.as_str().unwrap_or("");
                let transformed_event = transformer::apply_transformer(transformer_config, message);
                let transformed_str = serde_json::to_string(&transformed_event).unwrap();
                json!({
                    "eventMessage": msg,
                    "transformedEventMessage": transformed_str,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "transformedLogs": transformed,
            }))
            .unwrap(),
        ))
    }

    // ---- Storage Tier Policy ----

    /// Account-level storage-tier policy. AWS accepts `STANDARD` and
    /// `INTELLIGENT_TIERING`; the value governs how newly ingested data is tiered.
    pub(crate) fn put_storage_tier_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let tier = body["storageTier"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "storageTier is required",
            )
        })?;
        if tier != "STANDARD" && tier != "INTELLIGENT_TIERING" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("storageTier must be STANDARD or INTELLIGENT_TIERING, got {tier}"),
            ));
        }
        let now = Utc::now().timestamp_millis();
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        state.storage_tier = Some(tier.to_string());
        state.storage_tier_last_updated = Some(now);
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "storageTier": tier,
                "lastUpdatedTime": now,
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn get_storage_tier_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let mut out = json!({});
        if let Some(state) = accounts.regional(&req.account_id, &req.region) {
            // Default to STANDARD when no policy has been set, matching AWS.
            let tier = state
                .storage_tier
                .clone()
                .unwrap_or_else(|| "STANDARD".to_string());
            out["storageTier"] = json!(tier);
            if let Some(ts) = state.storage_tier_last_updated {
                out["lastUpdatedTime"] = json!(ts);
            }
        } else {
            out["storageTier"] = json!("STANDARD");
        }
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&out).unwrap(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- Account policies ----

    #[test]
    fn account_policy_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "PutAccountPolicy",
            json!({
                "policyName": "test-policy",
                "policyType": "DATA_PROTECTION_POLICY",
                "policyDocument": "{\"Name\":\"test\"}",
            }),
        );
        let resp = svc.put_account_policy(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["accountPolicy"]["policyName"], "test-policy");

        let req = make_request(
            "DescribeAccountPolicies",
            json!({ "policyType": "DATA_PROTECTION_POLICY" }),
        );
        let resp = svc.describe_account_policies(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["accountPolicies"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DeleteAccountPolicy",
            json!({
                "policyName": "test-policy",
                "policyType": "DATA_PROTECTION_POLICY",
            }),
        );
        svc.delete_account_policy(&req).unwrap();

        let req = make_request(
            "DescribeAccountPolicies",
            json!({ "policyType": "DATA_PROTECTION_POLICY" }),
        );
        let resp = svc.describe_account_policies(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["accountPolicies"].as_array().unwrap().is_empty());
    }

    // ---- Data protection policy ----

    #[test]
    fn data_protection_policy_lifecycle() {
        let svc = make_service();
        create_group(&svc, "dp-group");

        let req = make_request(
            "PutDataProtectionPolicy",
            json!({
                "logGroupIdentifier": "dp-group",
                "policyDocument": "{\"Name\":\"dp\"}",
            }),
        );
        svc.put_data_protection_policy(&req).unwrap();

        let req = make_request(
            "GetDataProtectionPolicy",
            json!({ "logGroupIdentifier": "dp-group" }),
        );
        let resp = svc.get_data_protection_policy(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["policyDocument"], "{\"Name\":\"dp\"}");

        let req = make_request(
            "DeleteDataProtectionPolicy",
            json!({ "logGroupIdentifier": "dp-group" }),
        );
        svc.delete_data_protection_policy(&req).unwrap();

        let req = make_request(
            "GetDataProtectionPolicy",
            json!({ "logGroupIdentifier": "dp-group" }),
        );
        let resp = svc.get_data_protection_policy(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body.get("policyDocument").is_none());
    }

    // ---- Index policies ----

    #[test]
    fn index_policy_lifecycle() {
        let svc = make_service();
        create_group(&svc, "idx-group");

        let req = make_request(
            "PutIndexPolicy",
            json!({
                "logGroupIdentifier": "idx-group",
                "policyDocument": "{\"Fields\":[\"field1\"]}",
            }),
        );
        svc.put_index_policy(&req).unwrap();

        let req = make_request(
            "DescribeIndexPolicies",
            json!({ "logGroupIdentifiers": ["idx-group"] }),
        );
        let resp = svc.describe_index_policies(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["indexPolicies"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DeleteIndexPolicy",
            json!({
                "logGroupIdentifier": "idx-group",
            }),
        );
        svc.delete_index_policy(&req).unwrap();
    }

    // ---- Transformers ----

    #[test]
    fn transformer_lifecycle() {
        let svc = make_service();
        create_group(&svc, "tx-group");

        let req = make_request(
            "PutTransformer",
            json!({
                "logGroupIdentifier": "tx-group",
                "transformerConfig": [{"addKeys":{"entries":[{"key":"new","value":"val"}]}}],
            }),
        );
        svc.put_transformer(&req).unwrap();

        let req = make_request(
            "GetTransformer",
            json!({ "logGroupIdentifier": "tx-group" }),
        );
        let resp = svc.get_transformer(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["transformerConfig"].is_array());

        let req = make_request(
            "DeleteTransformer",
            json!({ "logGroupIdentifier": "tx-group" }),
        );
        svc.delete_transformer(&req).unwrap();
    }

    #[test]
    fn test_transformer_returns_transformed_events() {
        let svc = make_service();

        let req = make_request(
            "TestTransformer",
            json!({
                "transformerConfig": [{"addKeys":{"entries":[]}}],
                "logEventMessages": ["hello", "world"],
            }),
        );
        let resp = svc.test_transformer(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["transformedLogs"].as_array().unwrap().len(), 2);
    }

    // ---- Resource policy ----

    #[test]
    fn resource_policy_lifecycle() {
        let svc = make_service();
        let req = make_request(
            "PutResourcePolicy",
            json!({
                "policyName": "p1",
                "policyDocument": "{}",
            }),
        );
        svc.put_resource_policy(&req).unwrap();

        let req = make_request("DescribeResourcePolicies", json!({}));
        let resp = svc.describe_resource_policies(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["resourcePolicies"].as_array().unwrap().len(), 1);

        let req = make_request("DeleteResourcePolicy", json!({"policyName": "p1"}));
        svc.delete_resource_policy(&req).unwrap();
    }

    #[test]
    fn put_resource_policy_missing_name_errors() {
        let svc = make_service();
        let req = make_request("PutResourcePolicy", json!({"policyDocument": "{}"}));
        assert!(svc.put_resource_policy(&req).is_err());
    }

    #[test]
    fn put_resource_policy_missing_document_errors() {
        let svc = make_service();
        let req = make_request("PutResourcePolicy", json!({"policyName": "p"}));
        assert!(svc.put_resource_policy(&req).is_err());
    }

    #[test]
    fn delete_resource_policy_missing_name_errors() {
        let svc = make_service();
        let req = make_request("DeleteResourcePolicy", json!({}));
        assert!(svc.delete_resource_policy(&req).is_err());
    }

    // ---- Account policy validation paths ----

    #[test]
    fn put_account_policy_missing_name_errors() {
        let svc = make_service();
        let req = make_request(
            "PutAccountPolicy",
            json!({"policyType": "DATA_PROTECTION_POLICY", "policyDocument": "{}"}),
        );
        assert!(svc.put_account_policy(&req).is_err());
    }

    #[test]
    fn put_account_policy_missing_type_errors() {
        let svc = make_service();
        let req = make_request(
            "PutAccountPolicy",
            json!({"policyName": "p", "policyDocument": "{}"}),
        );
        assert!(svc.put_account_policy(&req).is_err());
    }

    #[test]
    fn describe_account_policies_missing_type_errors() {
        let svc = make_service();
        let req = make_request("DescribeAccountPolicies", json!({}));
        assert!(svc.describe_account_policies(&req).is_err());
    }

    #[test]
    fn delete_account_policy_missing_fields_errors() {
        let svc = make_service();
        let req = make_request("DeleteAccountPolicy", json!({"policyName": "p"}));
        assert!(svc.delete_account_policy(&req).is_err());
    }

    // ---- Data protection policy error paths ----

    #[test]
    fn put_data_protection_missing_identifier_errors() {
        let svc = make_service();
        let req = make_request("PutDataProtectionPolicy", json!({"policyDocument": "{}"}));
        assert!(svc.put_data_protection_policy(&req).is_err());
    }

    #[test]
    fn put_data_protection_unknown_group_errors() {
        let svc = make_service();
        let req = make_request(
            "PutDataProtectionPolicy",
            json!({"logGroupIdentifier": "missing", "policyDocument": "{}"}),
        );
        assert!(svc.put_data_protection_policy(&req).is_err());
    }

    #[test]
    fn get_data_protection_missing_identifier_errors() {
        let svc = make_service();
        let req = make_request("GetDataProtectionPolicy", json!({}));
        assert!(svc.get_data_protection_policy(&req).is_err());
    }

    #[test]
    fn delete_data_protection_missing_identifier_errors() {
        let svc = make_service();
        let req = make_request("DeleteDataProtectionPolicy", json!({}));
        assert!(svc.delete_data_protection_policy(&req).is_err());
    }

    // ---- Index policy error paths ----

    #[test]
    fn put_index_policy_missing_identifier_errors() {
        let svc = make_service();
        let req = make_request("PutIndexPolicy", json!({"policyDocument": "{}"}));
        assert!(svc.put_index_policy(&req).is_err());
    }

    #[test]
    fn delete_index_policy_missing_identifier_errors() {
        let svc = make_service();
        let req = make_request("DeleteIndexPolicy", json!({}));
        assert!(svc.delete_index_policy(&req).is_err());
    }

    // ---- Transformer error paths ----

    #[test]
    fn put_transformer_missing_identifier_errors() {
        let svc = make_service();
        let req = make_request("PutTransformer", json!({"transformerConfig": []}));
        assert!(svc.put_transformer(&req).is_err());
    }

    #[test]
    fn get_transformer_missing_identifier_errors() {
        let svc = make_service();
        let req = make_request("GetTransformer", json!({}));
        assert!(svc.get_transformer(&req).is_err());
    }

    #[test]
    fn delete_transformer_missing_identifier_errors() {
        let svc = make_service();
        let req = make_request("DeleteTransformer", json!({}));
        assert!(svc.delete_transformer(&req).is_err());
    }

    #[test]
    fn describe_field_indexes_basic() {
        let svc = make_service();
        create_group(&svc, "g");
        let req = make_request(
            "DescribeFieldIndexes",
            json!({"logGroupIdentifiers": ["g"]}),
        );
        let resp = svc.describe_field_indexes(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["fieldIndexes"].as_array().unwrap().is_empty());
    }

    #[test]
    fn describe_field_indexes_reports_and_filters_by_index_category() {
        let svc = make_service();
        create_group(&svc, "g");
        let req = make_request(
            "PutIndexPolicy",
            json!({
                "logGroupIdentifier": "g",
                "policyDocument": "{\"Fields\":[\"userId\"]}"
            }),
        );
        svc.put_index_policy(&req).unwrap();

        // Every index fakecloud reports comes from a customer index policy.
        let req = make_request(
            "DescribeFieldIndexes",
            json!({"logGroupIdentifiers": ["g"]}),
        );
        let resp = svc.describe_field_indexes(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["fieldIndexes"][0]["indexCategory"], "CUSTOM");
        assert_eq!(body["fieldIndexes"][0]["type"], "FIELD_INDEX");

        // Asking for CUSTOM keeps it; asking only for other categories drops it.
        let req = make_request(
            "DescribeFieldIndexes",
            json!({"logGroupIdentifiers": ["g"], "indexCategories": ["CUSTOM"]}),
        );
        let resp = svc.describe_field_indexes(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["fieldIndexes"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DescribeFieldIndexes",
            json!({"logGroupIdentifiers": ["g"], "indexCategories": ["DEFAULT", "AUTO"]}),
        );
        let resp = svc.describe_field_indexes(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["fieldIndexes"].as_array().unwrap().is_empty());

        // Index policies still list normally — the filter is field-index only.
        let req = make_request(
            "DescribeIndexPolicies",
            json!({"logGroupIdentifiers": ["g"]}),
        );
        let resp = svc.describe_index_policies(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["indexPolicies"].as_array().unwrap().len(), 1);

        // Values outside the enum, and an over-long list, are rejected.
        let req = make_request(
            "DescribeFieldIndexes",
            json!({"logGroupIdentifiers": ["g"], "indexCategories": ["NOPE"]}),
        );
        assert!(svc.describe_field_indexes(&req).is_err());
        let req = make_request(
            "DescribeFieldIndexes",
            json!({
                "logGroupIdentifiers": ["g"],
                "indexCategories": ["DEFAULT", "CUSTOM", "AUTO", "INACTIVE", "CUSTOM"]
            }),
        );
        assert!(svc.describe_field_indexes(&req).is_err());
    }

    #[test]
    fn describe_field_indexes_returns_parsed_fields_from_index_policy() {
        let svc = make_service();
        create_group(&svc, "g");
        let req = make_request(
            "PutIndexPolicy",
            json!({
                "logGroupIdentifier": "g",
                "policyDocument": "{\"Fields\":[\"@timestamp\",\"userId\",\"requestId\"]}"
            }),
        );
        svc.put_index_policy(&req).unwrap();

        let req = make_request(
            "DescribeFieldIndexes",
            json!({"logGroupIdentifiers": ["g"]}),
        );
        let resp = svc.describe_field_indexes(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arr = body["fieldIndexes"].as_array().unwrap();
        assert_eq!(arr.len(), 3);
        let names: Vec<&str> = arr
            .iter()
            .map(|v| v["fieldIndexName"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"@timestamp"));
        assert!(names.contains(&"userId"));
        assert!(names.contains(&"requestId"));
    }

    // ---- Account-level retention policies ----

    fn describe_groups(svc: &crate::service::LogsService) -> Value {
        let resp = svc
            .describe_log_groups(&make_request("DescribeLogGroups", json!({})))
            .unwrap();
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn group<'a>(body: &'a Value, name: &str) -> &'a Value {
        body["logGroups"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["logGroupName"] == name)
            .unwrap()
    }

    fn put_retention(
        svc: &crate::service::LogsService,
        name: &str,
        doc: &str,
        criteria: Option<&str>,
        dry_run: bool,
    ) -> Result<Value, fakecloud_core::service::AwsServiceError> {
        let mut body = json!({
            "policyName": name,
            "policyType": "RETENTION_POLICY",
            "policyDocument": doc,
            "dryRun": dry_run,
        });
        if let Some(c) = criteria {
            body["selectionCriteria"] = json!(c);
        }
        svc.put_account_policy(&make_request("PutAccountPolicy", body))
            .map(|r| serde_json::from_slice(r.body.expect_bytes()).unwrap())
    }

    #[test]
    fn retention_policy_dry_run_previews_without_saving() {
        let svc = make_service();
        create_group(&svc, "/app/a");
        create_group(&svc, "/app/b");
        create_group(&svc, "/other");
        svc.put_retention_policy(&make_request(
            "PutRetentionPolicy",
            json!({ "logGroupName": "/app/a", "retentionInDays": 30 }),
        ))
        .unwrap();

        let body = put_retention(
            &svc,
            "app-90",
            r#"{"retentionInDays": 90, "enforcementMode": "RETROACTIVE"}"#,
            Some(r#"LogGroupNamePrefix IN ["/app"]"#),
            true,
        )
        .unwrap();
        assert!(body.get("accountPolicy").is_none(), "{body}");
        let dr = &body["dryRunResult"];
        // /app/a: 30 -> 90 is longer; /app/b: never-expire -> 90 is shorter.
        assert_eq!(dr["totalMatchedLogGroups"], 2);
        assert_eq!(dr["increasedRetentionLogGroupCount"], 1);
        assert_eq!(dr["decreasedRetentionLogGroupCount"], 1);
        assert_eq!(dr["unchangedRetentionLogGroupCount"], 0);
        assert_eq!(dr["failedAnalysisCount"], 0);
        let impacts = dr["topLogGroupsImpacted"].as_array().unwrap();
        let a = impacts
            .iter()
            .find(|i| i["logGroupName"] == "/app/a")
            .unwrap();
        assert_eq!(a["currentRetention"], "30");
        assert_eq!(a["proposedRetention"], "90");
        let b = impacts
            .iter()
            .find(|i| i["logGroupName"] == "/app/b")
            .unwrap();
        assert_eq!(b["currentRetention"], "NEVER_EXPIRE");

        // Nothing was saved.
        let resp = svc
            .describe_account_policies(&make_request(
                "DescribeAccountPolicies",
                json!({ "policyType": "RETENTION_POLICY" }),
            ))
            .unwrap();
        let listed: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(listed["accountPolicies"].as_array().unwrap().is_empty());
        let groups = describe_groups(&svc);
        assert_eq!(group(&groups, "/app/a")["retentionInDays"], 30);
        assert!(group(&groups, "/app/a")
            .get("retentionPolicyName")
            .is_none());
    }

    #[test]
    fn retention_policy_applies_longest_and_surfaces_policy_name() {
        let svc = make_service();
        create_group(&svc, "/app/a");
        create_group(&svc, "/other");
        svc.put_retention_policy(&make_request(
            "PutRetentionPolicy",
            json!({ "logGroupName": "/other", "retentionInDays": 7 }),
        ))
        .unwrap();

        let body = put_retention(
            &svc,
            "baseline",
            r#"{"retentionInDays": 30, "enforcementMode": "RETROACTIVE"}"#,
            None,
            false,
        )
        .unwrap();
        assert_eq!(body["accountPolicy"]["policyType"], "RETENTION_POLICY");
        put_retention(
            &svc,
            "app-forever",
            r#"{"retentionInDays": "NEVER_EXPIRE", "enforcementMode": "RETROACTIVE"}"#,
            Some(r#"LogGroupNamePrefix IN ["/app"]"#),
            false,
        )
        .unwrap();

        let groups = describe_groups(&svc);
        // The account policy replaces the group's own 7 days.
        let other = group(&groups, "/other");
        assert_eq!(other["retentionInDays"], 30);
        assert_eq!(other["retentionPolicyName"], "baseline");
        // NEVER_EXPIRE is the longest and omits retentionInDays.
        let app = group(&groups, "/app/a");
        assert!(app.get("retentionInDays").is_none(), "{app}");
        assert_eq!(app["retentionPolicyName"], "app-forever");

        // Dry-run delete previews the fallback, delete really removes it.
        let resp = svc
            .delete_account_policy(&make_request(
                "DeleteAccountPolicy",
                json!({ "policyName": "baseline", "policyType": "RETENTION_POLICY", "dryRun": true }),
            ))
            .unwrap();
        let dr: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        // /other falls back to 7 days (shorter); /app/a stays NEVER_EXPIRE.
        assert_eq!(dr["dryRunResult"]["totalMatchedLogGroups"], 2);
        assert_eq!(dr["dryRunResult"]["decreasedRetentionLogGroupCount"], 1);
        assert_eq!(dr["dryRunResult"]["unchangedRetentionLogGroupCount"], 1);
        assert_eq!(
            group(&describe_groups(&svc), "/other")["retentionPolicyName"],
            "baseline"
        );
        svc.delete_account_policy(&make_request(
            "DeleteAccountPolicy",
            json!({ "policyName": "baseline", "policyType": "RETENTION_POLICY" }),
        ))
        .unwrap();
        let other = group(&describe_groups(&svc), "/other").clone();
        assert_eq!(other["retentionInDays"], 7);
        assert!(other.get("retentionPolicyName").is_none());
    }

    #[test]
    fn retention_policy_new_only_skips_existing_groups() {
        let svc = make_service();
        create_group(&svc, "/old");
        std::thread::sleep(std::time::Duration::from_millis(2));
        put_retention(
            &svc,
            "new-only",
            r#"{"retentionInDays": 60, "enforcementMode": "NEW_ONLY"}"#,
            None,
            false,
        )
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        create_group(&svc, "/new");
        let groups = describe_groups(&svc);
        assert!(group(&groups, "/old").get("retentionPolicyName").is_none());
        assert_eq!(group(&groups, "/new")["retentionPolicyName"], "new-only");
        assert_eq!(group(&groups, "/new")["retentionInDays"], 60);
    }

    #[test]
    fn retention_policy_expires_events_by_policy_retention() {
        let svc = make_service();
        create_group(&svc, "/exp");
        create_stream(&svc, "/exp", "s");
        put_events(&svc, "/exp", "s", &["fresh"]);
        // An event older than the 14-day PutLogEvents window, as if it had
        // been stored long ago.
        {
            let mut accounts = svc.state.write();
            let stream = accounts
                .default_regional_mut()
                .log_groups
                .get_mut("/exp")
                .unwrap()
                .log_streams
                .get_mut("s")
                .unwrap();
            let mut stale = stream.events[0].clone();
            stale.timestamp -= 40 * 86_400_000;
            stale.message = "stale".into();
            stream.events.insert(0, stale);
        }
        put_retention(
            &svc,
            "thirty",
            r#"{"retentionInDays": 30, "enforcementMode": "RETROACTIVE"}"#,
            None,
            false,
        )
        .unwrap();
        let resp = svc
            .get_log_events(&make_request(
                "GetLogEvents",
                json!({ "logGroupName": "/exp", "logStreamName": "s" }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let messages: Vec<&str> = body["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["message"].as_str().unwrap())
            .collect();
        assert_eq!(messages, ["fresh"], "{body}");
    }

    #[test]
    fn retention_policy_validation() {
        let svc = make_service();
        // Below the 30-day account-policy minimum.
        let err = put_retention(
            &svc,
            "p",
            r#"{"retentionInDays": 14, "enforcementMode": "RETROACTIVE"}"#,
            None,
            false,
        )
        .err()
        .unwrap();
        assert_eq!(err.code(), "InvalidParameterException");
        // Only LogGroupNamePrefix criteria.
        let err = put_retention(
            &svc,
            "p",
            r#"{"retentionInDays": 30, "enforcementMode": "RETROACTIVE"}"#,
            Some(r#"LogGroupName IN ["x"]"#),
            false,
        )
        .err()
        .unwrap();
        assert_eq!(err.code(), "InvalidParameterException");
        // dryRun is only valid for retention policies.
        let err = svc
            .put_account_policy(&make_request(
                "PutAccountPolicy",
                json!({
                    "policyName": "dp",
                    "policyType": "DATA_PROTECTION_POLICY",
                    "policyDocument": "{}",
                    "dryRun": true,
                }),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidParameterException");
        // At most 20 retention policies per Region.
        for i in 0..20 {
            put_retention(
                &svc,
                &format!("p{i}"),
                r#"{"retentionInDays": 30, "enforcementMode": "RETROACTIVE"}"#,
                None,
                false,
            )
            .unwrap();
        }
        let err = put_retention(
            &svc,
            "p20",
            r#"{"retentionInDays": 30, "enforcementMode": "RETROACTIVE"}"#,
            None,
            false,
        )
        .err()
        .unwrap();
        assert_eq!(err.code(), "LimitExceededException");
        // Updating an existing one is still allowed.
        put_retention(
            &svc,
            "p0",
            r#"{"retentionInDays": 60, "enforcementMode": "RETROACTIVE"}"#,
            None,
            false,
        )
        .unwrap();
    }

    #[test]
    fn describe_log_groups_renders_protection_and_class_members() {
        let svc = make_service();
        svc.create_log_group(&make_request(
            "CreateLogGroup",
            json!({ "logGroupName": "/ess", "logGroupClass": "ESSENTIALS" }),
        ))
        .unwrap();
        svc.put_log_group_deletion_protection(&make_request(
            "PutLogGroupDeletionProtection",
            json!({ "logGroupIdentifier": "/ess", "deletionProtectionEnabled": true }),
        ))
        .unwrap();
        svc.put_bearer_token_authentication(&make_request(
            "PutBearerTokenAuthentication",
            json!({ "logGroupIdentifier": "/ess", "bearerTokenAuthenticationEnabled": true }),
        ))
        .unwrap();
        svc.put_data_protection_policy(&make_request(
            "PutDataProtectionPolicy",
            json!({ "logGroupIdentifier": "/ess", "policyDocument": "{}" }),
        ))
        .unwrap();
        let g = group(&describe_groups(&svc), "/ess").clone();
        assert_eq!(g["logGroupClass"], "ESSENTIALS");
        assert_eq!(g["deletionProtectionEnabled"], true);
        assert_eq!(g["bearerTokenAuthenticationEnabled"], true);
        assert_eq!(g["dataProtectionStatus"], "ACTIVATED");
        assert!(g.get("inheritedProperties").is_none());

        svc.delete_data_protection_policy(&make_request(
            "DeleteDataProtectionPolicy",
            json!({ "logGroupIdentifier": "/ess" }),
        ))
        .unwrap();
        svc.put_account_policy(&make_request(
            "PutAccountPolicy",
            json!({
                "policyName": "acct-dp",
                "policyType": "DATA_PROTECTION_POLICY",
                "policyDocument": "{}",
            }),
        ))
        .unwrap();
        let g = group(&describe_groups(&svc), "/ess").clone();
        assert_eq!(g["dataProtectionStatus"], "DELETED");
        assert_eq!(g["inheritedProperties"], json!(["ACCOUNT_DATA_PROTECTION"]));
    }
}
