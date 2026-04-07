use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use chrono::Utc;

use super::{body_json, LogsService};
use super::{extract_log_group_from_arn, resolve_log_group_name};

use crate::state::LogGroup;

impl LogsService {
    // ---- Log Groups ----

    pub(crate) fn create_log_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupName is required",
                )
            })?
            .to_string();

        validate_string_length("logGroupName", &name, 1, 512)?;
        validate_optional_string_length("kmsKeyId", body["kmsKeyId"].as_str(), 1, 256)?;

        let mut state = self.state.write();
        if state.log_groups.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceAlreadyExistsException",
                format!("The specified log group already exists: {name}"),
            ));
        }

        let arn = format!(
            "arn:aws:logs:{}:{}:log-group:{}:*",
            state.region, state.account_id, name
        );
        let now = Utc::now().timestamp_millis();

        let tags = body["tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let kms_key_id = body["kmsKeyId"].as_str().map(|s| s.to_string());

        state.log_groups.insert(
            name.clone(),
            LogGroup {
                name,
                arn,
                creation_time: now,
                retention_in_days: None,
                kms_key_id,
                tags,
                log_streams: std::collections::HashMap::new(),
                stored_bytes: 0,
                subscription_filters: Vec::new(),
                data_protection_policy: None,
                index_policies: Vec::new(),
                transformer: None,
                deletion_protection: false,
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn delete_log_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", name, 1, 512)?;

        let mut state = self.state.write();
        // Check deletion protection
        if let Some(group) = state.log_groups.get(name) {
            if group.deletion_protection {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "OperationAbortedException",
                    format!("Log group {name} has deletion protection enabled"),
                ));
            }
        }
        if state.log_groups.remove(name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn describe_log_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let prefix = body["logGroupNamePrefix"].as_str().unwrap_or("");
        let pattern = body["logGroupNamePattern"].as_str().unwrap_or("");
        let limit = body["limit"].as_i64().unwrap_or(50) as usize;
        let next_token = body["nextToken"].as_str();

        validate_optional_string_length(
            "logGroupNamePrefix",
            body["logGroupNamePrefix"].as_str(),
            1,
            512,
        )?;
        validate_optional_string_length(
            "logGroupNamePattern",
            body["logGroupNamePattern"].as_str(),
            0,
            512,
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "logGroupClass",
            &body["logGroupClass"],
            &["STANDARD", "INFREQUENT_ACCESS", "DELIVERY"],
        )?;

        let state = self.state.read();
        let mut groups: Vec<&LogGroup> = state
            .log_groups
            .values()
            .filter(|g| {
                (prefix.is_empty() || g.name.starts_with(prefix))
                    && (pattern.is_empty() || g.name.contains(pattern))
            })
            .collect();
        groups.sort_by(|a, b| a.name.cmp(&b.name));

        // Handle pagination
        let start_idx = if let Some(token) = next_token {
            groups
                .iter()
                .position(|g| g.name.as_str() > token)
                .unwrap_or(groups.len())
        } else {
            0
        };

        let page = &groups[start_idx..];
        let has_more = page.len() > limit;
        let page = if has_more { &page[..limit] } else { page };

        let log_groups: Vec<Value> = page
            .iter()
            .map(|g| {
                let log_group_arn = g.arn.trim_end_matches(":*").to_string();
                let mut obj = json!({
                    "logGroupName": g.name,
                    "arn": g.arn,
                    "logGroupArn": log_group_arn,
                    "creationTime": g.creation_time,
                    "storedBytes": g.stored_bytes,
                    "metricFilterCount": 0,
                });
                if let Some(days) = g.retention_in_days {
                    obj["retentionInDays"] = json!(days);
                }
                if let Some(ref kms) = g.kms_key_id {
                    obj["kmsKeyId"] = json!(kms);
                }
                obj
            })
            .collect();

        let mut result = json!({ "logGroups": log_groups });
        if has_more {
            if let Some(last) = page.last() {
                result["nextToken"] = json!(last.name);
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    // ---- Retention Policy ----

    pub(crate) fn put_retention_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", name, 1, 512)?;

        let days = body["retentionInDays"].as_i64().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "retentionInDays is required",
            )
        })?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            )
        })?;

        group.retention_in_days = Some(days as i32);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn delete_retention_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", name, 1, 512)?;

        let mut state = self.state.write();
        let group = state.log_groups.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {name}"),
            )
        })?;

        group.retention_in_days = None;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ---- KMS Key ----

    pub(crate) fn associate_kms_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str();
        let resource_identifier = body["resourceIdentifier"].as_str();
        let kms_key_id = body["kmsKeyId"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "kmsKeyId is required",
                )
            })?
            .to_string();

        if let Some(name) = log_group_name {
            validate_string_length("logGroupName", name, 1, 512)?;
        }
        validate_string_length("kmsKeyId", &kms_key_id, 1, 256)?;
        validate_optional_string_length("resourceIdentifier", resource_identifier, 1, 2048)?;

        let resolved_name = resolve_log_group_name(log_group_name, resource_identifier)?;

        let mut state = self.state.write();
        let group = state
            .log_groups
            .get_mut(resolved_name.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("The specified log group does not exist: {resolved_name}"),
                )
            })?;

        group.kms_key_id = Some(kms_key_id);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn disassociate_kms_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str();
        let resource_identifier = body["resourceIdentifier"].as_str();

        if let Some(name) = log_group_name {
            validate_string_length("logGroupName", name, 1, 512)?;
        }
        validate_optional_string_length("resourceIdentifier", resource_identifier, 1, 2048)?;

        let resolved_name = resolve_log_group_name(log_group_name, resource_identifier)?;

        let mut state = self.state.write();
        let group = state
            .log_groups
            .get_mut(resolved_name.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("The specified log group does not exist: {resolved_name}"),
                )
            })?;

        group.kms_key_id = None;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn get_log_group_fields(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_id = body["logGroupName"]
            .as_str()
            .or_else(|| body["logGroupIdentifier"].as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupName or logGroupIdentifier is required",
                )
            })?;

        let group_name = if log_group_id.starts_with("arn:") {
            extract_log_group_from_arn(log_group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Invalid ARN: {log_group_id}"),
                )
            })?
        } else {
            log_group_id.to_string()
        };

        let state = self.state.read();
        if !state.log_groups.contains_key(&group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            ));
        }

        // Stub response with common fields
        let fields = json!([
            { "fieldName": "@timestamp", "percent": 100 },
            { "fieldName": "@message", "percent": 100 },
        ]);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logGroupFields": fields })).unwrap(),
        ))
    }

    pub(crate) fn put_log_group_deletion_protection(
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
        let deletion_protection = body["deletionProtectionEnabled"].as_bool().unwrap_or(true);

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

        group.deletion_protection = deletion_protection;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn list_aggregate_log_group_summaries(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("groupBy", &body["groupBy"])?;
        validate_optional_enum_value(
            "groupBy",
            &body["groupBy"],
            &[
                "DATA_SOURCE_NAME_TYPE_AND_FORMAT",
                "DATA_SOURCE_NAME_AND_TYPE",
            ],
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_enum_value(
            "logGroupClass",
            &body["logGroupClass"],
            &["STANDARD", "INFREQUENT_ACCESS", "DELIVERY"],
        )?;
        validate_optional_string_length(
            "logGroupNamePattern",
            body["logGroupNamePattern"].as_str(),
            3,
            129,
        )?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        // Stub: return empty summaries
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "aggregateLogGroupSummaries": [] })).unwrap(),
        ))
    }

    pub(crate) fn list_log_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let prefix = body["logGroupNamePrefix"].as_str().unwrap_or("");
        let pattern = body["logGroupNamePattern"].as_str().unwrap_or("");
        let limit = body["limit"].as_i64().unwrap_or(50) as usize;
        let next_token = body["nextToken"].as_str();

        validate_optional_string_length(
            "logGroupNamePrefix",
            body["logGroupNamePrefix"].as_str(),
            1,
            512,
        )?;
        validate_optional_string_length(
            "logGroupNamePattern",
            body["logGroupNamePattern"].as_str(),
            3,
            129,
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 1000)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        validate_optional_enum_value(
            "logGroupClass",
            &body["logGroupClass"],
            &["STANDARD", "INFREQUENT_ACCESS", "DELIVERY"],
        )?;

        let state = self.state.read();
        let mut groups: Vec<&LogGroup> = state
            .log_groups
            .values()
            .filter(|g| {
                (prefix.is_empty() || g.name.starts_with(prefix))
                    && (pattern.is_empty() || g.name.contains(pattern))
            })
            .collect();
        groups.sort_by(|a, b| a.name.cmp(&b.name));

        let start_idx = if let Some(token) = next_token {
            groups
                .iter()
                .position(|g| g.name.as_str() > token)
                .unwrap_or(groups.len())
        } else {
            0
        };

        let page = &groups[start_idx..];
        let has_more = page.len() > limit;
        let page = if has_more { &page[..limit] } else { page };

        // ListLogGroups returns LogGroupSummary (logGroupName, logGroupArn, logGroupClass only)
        let log_groups: Vec<Value> = page
            .iter()
            .map(|g| {
                let log_group_arn = g.arn.trim_end_matches(":*").to_string();
                json!({
                    "logGroupName": g.name,
                    "logGroupArn": log_group_arn,
                    "logGroupClass": "STANDARD",
                })
            })
            .collect();

        let mut result = json!({ "logGroups": log_groups });
        if has_more {
            if let Some(last) = page.last() {
                result["nextToken"] = json!(last.name);
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- describe_log_groups: logGroupNamePattern ----

    #[test]
    fn describe_log_groups_pattern_filters_by_substring() {
        let svc = make_service();
        create_group(&svc, "/app/web");
        create_group(&svc, "/app/api");
        create_group(&svc, "/system/metrics");

        let req = make_request("DescribeLogGroups", json!({ "logGroupNamePattern": "app" }));
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let names: Vec<&str> = body["logGroups"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| g["logGroupName"].as_str().unwrap())
            .collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"/app/web"));
        assert!(names.contains(&"/app/api"));
    }

    #[test]
    fn describe_log_groups_pattern_empty_returns_all() {
        let svc = make_service();
        create_group(&svc, "/app/web");
        create_group(&svc, "/system/metrics");

        let req = make_request("DescribeLogGroups", json!({}));
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["logGroups"].as_array().unwrap().len(), 2);
    }

    // ---- associate_kms_key / disassociate_kms_key: resourceIdentifier ----

    #[test]
    fn associate_kms_key_via_resource_identifier_arn() {
        let svc = make_service();
        create_group(&svc, "grp");

        let req = make_request(
            "AssociateKmsKey",
            json!({
                "resourceIdentifier": "arn:aws:logs:us-east-1:123456789012:log-group:grp:*",
                "kmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/abc-123",
            }),
        );
        svc.associate_kms_key(&req).unwrap();

        let state = svc.state.read();
        assert_eq!(
            state.log_groups["grp"].kms_key_id.as_deref(),
            Some("arn:aws:kms:us-east-1:123456789012:key/abc-123")
        );
    }

    #[test]
    fn disassociate_kms_key_via_resource_identifier_name() {
        let svc = make_service();
        create_group(&svc, "grp");

        // First associate
        let req = make_request(
            "AssociateKmsKey",
            json!({ "logGroupName": "grp", "kmsKeyId": "some-key" }),
        );
        svc.associate_kms_key(&req).unwrap();

        // Disassociate via resourceIdentifier (plain name)
        let req = make_request("DisassociateKmsKey", json!({ "resourceIdentifier": "grp" }));
        svc.disassociate_kms_key(&req).unwrap();

        let state = svc.state.read();
        assert!(state.log_groups["grp"].kms_key_id.is_none());
    }
}
