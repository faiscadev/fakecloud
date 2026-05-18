use http::StatusCode;
use serde_json::{json, Value};

use crate::validation::*;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use chrono::Utc;

use super::LogsService;
use super::{extract_log_group_from_arn, resolve_log_group_name};

use crate::state::LogGroup;

impl LogsService {
    // ---- Log Groups ----

    pub(crate) fn create_log_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let log_group_class = body["logGroupClass"]
            .as_str()
            .map(|s| s.to_string())
            .or_else(|| Some("STANDARD".to_string()));

        state.log_groups.insert(
            name.clone(),
            LogGroup {
                name,
                arn,
                creation_time: now,
                retention_in_days: None,
                kms_key_id,
                tags,
                log_streams: std::collections::BTreeMap::new(),
                stored_bytes: 0,
                subscription_filters: Vec::new(),
                data_protection_policy: None,
                index_policies: Vec::new(),
                transformer: None,
                deletion_protection: false,
                log_group_class,
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn delete_log_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", name, 1, 512)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
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

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
                    // Real AWS DescribeLogGroups always returns logGroupClass.
                    // Terraform's `aws_cloudwatch_log_group` provider asserts
                    // `log_group_class == "STANDARD"` on every refresh, so
                    // omitting the field surfaces as drift / `expected
                    // STANDARD got ""` failures.
                    "logGroupClass": g
                        .log_group_class
                        .as_deref()
                        .unwrap_or("STANDARD"),
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
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
        let name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;

        validate_string_length("logGroupName", name, 1, 512)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
        let log_group_name = body["logGroupName"].as_str();
        let resource_identifier = body["resourceIdentifier"].as_str();

        if let Some(name) = log_group_name {
            validate_string_length("logGroupName", name, 1, 512)?;
        }
        validate_optional_string_length("resourceIdentifier", resource_identifier, 1, 2048)?;

        let resolved_name = resolve_log_group_name(log_group_name, resource_identifier)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
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

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let group = state.log_groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("The specified log group does not exist: {group_name}"),
            )
        })?;

        // Walk every event in every stream and tally how often each
        // discovered field appears. JSON-shaped events contribute their
        // top-level keys; every event always contributes @timestamp +
        // @message + @logStream.
        let mut total: u64 = 0;
        let mut counts: std::collections::BTreeMap<String, u64> = Default::default();
        for stream in group.log_streams.values() {
            for ev in &stream.events {
                total += 1;
                *counts.entry("@timestamp".to_string()).or_insert(0) += 1;
                *counts.entry("@message".to_string()).or_insert(0) += 1;
                *counts.entry("@logStream".to_string()).or_insert(0) += 1;
                if let Ok(serde_json::Value::Object(map)) =
                    serde_json::from_str::<serde_json::Value>(&ev.message)
                {
                    for k in map.keys() {
                        *counts.entry(k.clone()).or_insert(0) += 1;
                    }
                }
            }
        }
        let denom = total.max(1) as f64;
        let mut fields: Vec<Value> = counts
            .into_iter()
            .map(|(name, n)| {
                let percent = ((n as f64 / denom) * 100.0).round() as i64;
                json!({ "name": name, "percent": percent })
            })
            .collect();
        // No events yet: still surface the always-present synthetic fields.
        if total == 0 {
            fields = vec![
                json!({ "name": "@timestamp", "percent": 100 }),
                json!({ "name": "@message", "percent": 100 }),
                json!({ "name": "@logStream", "percent": 100 }),
            ];
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logGroupFields": fields })).unwrap(),
        ))
    }

    pub(crate) fn put_log_group_deletion_protection(
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
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
        let body = req.json_body();
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

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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

        let _mas = svc.state.read();
        let state = _mas.default_ref();
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

        let _mas = svc.state.read();
        let state = _mas.default_ref();
        assert!(state.log_groups["grp"].kms_key_id.is_none());
    }

    // ---- create_log_group ----

    #[test]
    fn create_log_group_duplicate_errors() {
        let svc = make_service();
        create_group(&svc, "dup");
        let req = make_request("CreateLogGroup", json!({"logGroupName": "dup"}));
        assert!(svc.create_log_group(&req).is_err());
    }

    #[test]
    fn create_log_group_missing_name_errors() {
        let svc = make_service();
        let req = make_request("CreateLogGroup", json!({}));
        assert!(svc.create_log_group(&req).is_err());
    }

    #[test]
    fn create_log_group_with_kms_and_tags() {
        let svc = make_service();
        let req = make_request(
            "CreateLogGroup",
            json!({
                "logGroupName": "/secure/app",
                "kmsKeyId": "arn:aws:kms:us-east-1:123:key/k1",
                "tags": {"env": "prod"}
            }),
        );
        svc.create_log_group(&req).unwrap();
        let mas = svc.state.read();
        let state = mas.default_ref();
        let grp = state.log_groups.get("/secure/app").unwrap();
        assert_eq!(
            grp.kms_key_id.as_deref(),
            Some("arn:aws:kms:us-east-1:123:key/k1")
        );
        assert_eq!(grp.tags.get("env").map(String::as_str), Some("prod"));
    }

    // ---- delete_log_group ----

    #[test]
    fn delete_log_group_unknown_errors() {
        let svc = make_service();
        let req = make_request("DeleteLogGroup", json!({"logGroupName": "missing"}));
        assert!(svc.delete_log_group(&req).is_err());
    }

    #[test]
    fn delete_log_group_missing_name_errors() {
        let svc = make_service();
        let req = make_request("DeleteLogGroup", json!({}));
        assert!(svc.delete_log_group(&req).is_err());
    }

    #[test]
    fn delete_log_group_removes_group() {
        let svc = make_service();
        create_group(&svc, "gone");
        let req = make_request("DeleteLogGroup", json!({"logGroupName": "gone"}));
        svc.delete_log_group(&req).unwrap();
        assert!(!svc
            .state
            .read()
            .default_ref()
            .log_groups
            .contains_key("gone"));
    }

    // ---- put_retention_policy ----

    #[test]
    fn put_retention_policy_missing_name_errors() {
        let svc = make_service();
        let req = make_request("PutRetentionPolicy", json!({"retentionInDays": 7}));
        assert!(svc.put_retention_policy(&req).is_err());
    }

    #[test]
    fn put_retention_policy_unknown_group_errors() {
        let svc = make_service();
        let req = make_request(
            "PutRetentionPolicy",
            json!({"logGroupName": "missing", "retentionInDays": 7}),
        );
        assert!(svc.put_retention_policy(&req).is_err());
    }

    #[test]
    fn put_retention_policy_roundtrip() {
        let svc = make_service();
        create_group(&svc, "ret");
        let req = make_request(
            "PutRetentionPolicy",
            json!({"logGroupName": "ret", "retentionInDays": 30}),
        );
        svc.put_retention_policy(&req).unwrap();
        assert_eq!(
            svc.state.read().default_ref().log_groups["ret"].retention_in_days,
            Some(30)
        );
    }

    #[test]
    fn delete_retention_policy_clears_retention() {
        let svc = make_service();
        create_group(&svc, "dr");
        let put = make_request(
            "PutRetentionPolicy",
            json!({"logGroupName": "dr", "retentionInDays": 30}),
        );
        svc.put_retention_policy(&put).unwrap();
        let del = make_request("DeleteRetentionPolicy", json!({"logGroupName": "dr"}));
        svc.delete_retention_policy(&del).unwrap();
        assert!(svc.state.read().default_ref().log_groups["dr"]
            .retention_in_days
            .is_none());
    }

    #[test]
    fn delete_retention_policy_unknown_group_errors() {
        let svc = make_service();
        let req = make_request("DeleteRetentionPolicy", json!({"logGroupName": "missing"}));
        assert!(svc.delete_retention_policy(&req).is_err());
    }

    // ---- associate / disassociate error paths ----

    #[test]
    fn associate_kms_key_missing_kms_key_errors() {
        let svc = make_service();
        create_group(&svc, "a");
        let req = make_request("AssociateKmsKey", json!({"logGroupName": "a"}));
        assert!(svc.associate_kms_key(&req).is_err());
    }

    #[test]
    fn associate_kms_key_missing_group_errors() {
        let svc = make_service();
        let req = make_request(
            "AssociateKmsKey",
            json!({"logGroupName": "missing", "kmsKeyId": "k"}),
        );
        assert!(svc.associate_kms_key(&req).is_err());
    }

    #[test]
    fn disassociate_kms_key_missing_group_errors() {
        let svc = make_service();
        let req = make_request("DisassociateKmsKey", json!({"logGroupName": "missing"}));
        assert!(svc.disassociate_kms_key(&req).is_err());
    }

    // ---- deletion protection ----

    #[test]
    fn put_log_group_deletion_protection_sets_flag() {
        let svc = make_service();
        create_group(&svc, "prot");
        let req = make_request(
            "PutLogGroupDeletionProtection",
            json!({"logGroupIdentifier": "prot", "deletionProtection": "ENABLED"}),
        );
        svc.put_log_group_deletion_protection(&req).unwrap();
    }

    #[test]
    fn put_log_group_deletion_protection_missing_identifier_errors() {
        let svc = make_service();
        let req = make_request(
            "PutLogGroupDeletionProtection",
            json!({"deletionProtection": "ENABLED"}),
        );
        assert!(svc.put_log_group_deletion_protection(&req).is_err());
    }

    // ---- list_log_groups / list_aggregate ----

    #[test]
    fn list_log_groups_returns_all() {
        let svc = make_service();
        create_group(&svc, "a");
        create_group(&svc, "b");
        let req = make_request("ListLogGroups", json!({}));
        let resp = svc.list_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["logGroups"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn list_aggregate_log_group_summaries_missing_group_by_errors() {
        let svc = make_service();
        let req = make_request("ListAggregateLogGroupSummaries", json!({}));
        assert!(svc.list_aggregate_log_group_summaries(&req).is_err());
    }
}
