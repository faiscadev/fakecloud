use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::{body_json, LogsService};
use chrono::Utc;

use super::extract_log_group_from_arn;
use crate::state::{AccountPolicy, DataProtectionPolicy, IndexPolicy, ResourcePolicy, Transformer};
use crate::transformer;

impl LogsService {
    // ---- Resource Policies ----

    pub(crate) fn put_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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

        let mut state = self.state.write();

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
        let body = body_json(req);
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "policyScope",
            &body["policyScope"],
            &["ACCOUNT", "RESOURCE"],
        )?;
        let state = self.state.read();

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
        let body = body_json(req);
        let policy_name = body["policyName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyName is required",
            )
        })?;

        let mut state = self.state.write();
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
        let body = body_json(req);
        validate_optional_enum_value(
            "policyType",
            &body["policyType"],
            &[
                "DATA_PROTECTION_POLICY",
                "SUBSCRIPTION_FILTER_POLICY",
                "FIELD_INDEX_POLICY",
                "TRANSFORMER_POLICY",
                "METRIC_EXTRACTION_POLICY",
            ],
        )?;
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

        let now = Utc::now().timestamp_millis();
        let mut state = self.state.write();
        let account_id = state.account_id.clone();
        let scope = body["scope"].as_str().map(|s| s.to_string());
        let selection_criteria = body["selectionCriteria"].as_str().map(|s| s.to_string());

        let policy = AccountPolicy {
            policy_name: policy_name.to_string(),
            policy_type: policy_type.to_string(),
            policy_document: policy_document.to_string(),
            scope: scope.clone(),
            selection_criteria: selection_criteria.clone(),
            account_id: account_id.clone(),
            last_updated_time: now,
        };

        let key = (policy_name.to_string(), policy_type.to_string());
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
        let body = body_json(req);
        validate_optional_enum_value(
            "policyType",
            &body["policyType"],
            &[
                "DATA_PROTECTION_POLICY",
                "SUBSCRIPTION_FILTER_POLICY",
                "FIELD_INDEX_POLICY",
                "TRANSFORMER_POLICY",
                "METRIC_EXTRACTION_POLICY",
            ],
        )?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        let policy_type = body["policyType"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "policyType is required",
            )
        })?;
        let policy_name = body["policyName"].as_str();

        let state = self.state.read();
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
        let body = body_json(req);
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

        let key = (policy_name.to_string(), policy_type.to_string());
        let mut state = self.state.write();
        if state.account_policies.remove(&key).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Account policy {policy_name} of type {policy_type} not found"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Data Protection Policies ----

    pub(crate) fn put_data_protection_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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
        let mut state = self.state.write();
        let group = state.log_groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;
        let log_group_id_resp = group.arn.clone();

        group.data_protection_policy = Some(DataProtectionPolicy {
            policy_document: policy_document.clone(),
            last_updated_time: now,
        });

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
        let body = body_json(req);
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

        let state = self.state.read();
        let group = state.log_groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        let mut result = json!({
            "logGroupIdentifier": group.arn,
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
        let body = body_json(req);
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

        let mut state = self.state.write();
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
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- Index Policies ----

    pub(crate) fn put_index_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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
        let mut state = self.state.write();
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

        let result = json!({
            "indexPolicy": {
                "policyName": policy_name,
                "policyDocument": policy_document,
                "logGroupIdentifier": group.arn,
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
        let body = body_json(req);
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        let log_group_ids = body["logGroupIdentifiers"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupIdentifiers is required",
            )
        })?;

        let state = self.state.read();
        let mut policies = Vec::new();

        for id_val in log_group_ids {
            let id = id_val.as_str().unwrap_or("");
            let group_name = if id.starts_with("arn:") {
                extract_log_group_from_arn(id).unwrap_or_default()
            } else {
                id.to_string()
            };
            if let Some(group) = state.log_groups.get(&group_name) {
                for p in &group.index_policies {
                    policies.push(json!({
                        "policyName": p.policy_name,
                        "policyDocument": p.policy_document,
                        "logGroupIdentifier": group.arn,
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
        let body = body_json(req);
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

        let mut state = self.state.write();
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
        let body = body_json(req);
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        // Validate that logGroupIdentifiers is provided
        let _log_group_ids = body["logGroupIdentifiers"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupIdentifiers is required",
            )
        })?;

        // Stub: return empty list
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "fieldIndexes": [] })).unwrap(),
        ))
    }

    // ---- Transformers ----

    pub(crate) fn put_transformer(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
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
        let mut state = self.state.write();
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
        let body = body_json(req);
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

        let state = self.state.read();
        let group = state.log_groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        let mut result = json!({
            "logGroupIdentifier": group.arn,
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
        let body = body_json(req);
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

        let mut state = self.state.write();
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
        let body = body_json(req);
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
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["accountPolicy"]["policyName"], "test-policy");

        let req = make_request(
            "DescribeAccountPolicies",
            json!({ "policyType": "DATA_PROTECTION_POLICY" }),
        );
        let resp = svc.describe_account_policies(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
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
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
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
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
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
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
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
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
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
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
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
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["transformedLogs"].as_array().unwrap().len(), 2);
    }
}
