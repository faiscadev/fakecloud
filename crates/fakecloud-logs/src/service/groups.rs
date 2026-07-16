use http::StatusCode;
use serde_json::{json, Value};

use crate::validation::*;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use chrono::Utc;

use super::LogsService;
use super::{extract_log_group_from_arn, resolve_log_group_name};

use crate::state::LogGroup;

/// Ordered grouping key for `ListAggregateLogGroupSummaries`:
/// (dataSource.Name, dataSource.Type, optional dataSource.Format).
type DataSourceGroupKey = (String, String, Option<String>);

/// Derive a data-source name for a log group from its name.
///
/// fakecloud does not model per-log-group telemetry data sources, so the only
/// real signal available is the log group name. AWS-managed log groups follow
/// the `/aws/<service>/...` convention that identifies the originating service
/// (e.g. `/aws/lambda`, `/aws/vpc`); everything else is grouped by its leading
/// name segment. This is what `ListAggregateLogGroupSummaries` groups on.
fn derive_log_group_data_source(name: &str) -> String {
    let segs: Vec<&str> = name.split('/').filter(|s| !s.is_empty()).collect();
    if segs.is_empty() {
        return "custom".to_string();
    }
    if segs[0] == "aws" && segs.len() >= 2 {
        format!("/aws/{}", segs[1])
    } else {
        segs[0].to_string()
    }
}

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
                let metric_filter_count = state
                    .metric_filters
                    .iter()
                    .filter(|mf| mf.log_group_name == g.name)
                    .count();
                let mut obj = json!({
                    "logGroupName": g.name,
                    "arn": g.arn,
                    "logGroupArn": log_group_arn,
                    "creationTime": g.creation_time,
                    "storedBytes": g.stored_bytes,
                    "metricFilterCount": metric_filter_count,
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

        let group_by = body["groupBy"].as_str().unwrap_or("");
        let include_format = group_by == "DATA_SOURCE_NAME_TYPE_AND_FORMAT";
        let class_filter = body["logGroupClass"].as_str();
        let pattern = body["logGroupNamePattern"].as_str().unwrap_or("");
        let limit = body["limit"].as_i64().unwrap_or(50) as usize;
        let next_token = body["nextToken"].as_str();

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        // Aggregate the actual stored log groups by their derived data-source
        // characteristics. fakecloud stores raw plaintext events with no OCSF
        // transformation, so every group's Type is a plain LogGroup and its
        // Format is Plain; the Name is derived from the log group name. Under
        // both groupBy modes this collapses to grouping by data-source name.
        let mut counts: std::collections::BTreeMap<DataSourceGroupKey, i64> =
            std::collections::BTreeMap::new();
        for g in state.log_groups.values() {
            let class = g.log_group_class.as_deref().unwrap_or("STANDARD");
            if let Some(f) = class_filter {
                if class != f {
                    continue;
                }
            }
            if !pattern.is_empty() && !g.name.contains(pattern) {
                continue;
            }
            let ds_name = derive_log_group_data_source(&g.name);
            let key = (
                ds_name,
                "LogGroup".to_string(),
                include_format.then(|| "Plain".to_string()),
            );
            *counts.entry(key).or_insert(0) += 1;
        }

        let all: Vec<(DataSourceGroupKey, i64)> = counts.into_iter().collect();

        // Opaque integer offset token. An unresolvable/garbage token ends the
        // listing (empty page, no token) rather than restarting at offset 0,
        // which would loop a client that resumes while a token is present.
        let start = match next_token {
            Some(t) => t.parse::<usize>().unwrap_or(usize::MAX),
            None => 0,
        }
        .min(all.len());
        let end = (start + limit).min(all.len());

        let summaries: Vec<Value> = all[start..end]
            .iter()
            .map(|((ds_name, ds_type, ds_format), count)| {
                let mut ids = vec![
                    json!({ "key": "dataSource.Name", "value": ds_name }),
                    json!({ "key": "dataSource.Type", "value": ds_type }),
                ];
                if let Some(fmt) = ds_format {
                    ids.push(json!({ "key": "dataSource.Format", "value": fmt }));
                }
                json!({
                    "logGroupCount": count,
                    "groupingIdentifiers": ids,
                })
            })
            .collect();

        let mut result = json!({ "aggregateLogGroupSummaries": summaries });
        if end < all.len() {
            result["nextToken"] = json!(end.to_string());
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
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
                    // Render the group's actual stored class, matching
                    // DescribeLogGroups. CreateLogGroup persists
                    // INFREQUENT_ACCESS / DELIVERY, so hardcoding STANDARD
                    // reported the wrong class for those groups.
                    "logGroupClass": g
                        .log_group_class
                        .as_deref()
                        .unwrap_or("STANDARD"),
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

    fn create_group_with_class(svc: &crate::LogsService, name: &str, class: &str) {
        let req = make_request(
            "CreateLogGroup",
            json!({ "logGroupName": name, "logGroupClass": class }),
        );
        svc.create_log_group(&req).unwrap();
    }

    #[test]
    fn list_log_groups_reports_stored_class() {
        let svc = make_service();
        create_group_with_class(&svc, "/infrequent", "INFREQUENT_ACCESS");
        create_group(&svc, "/standard");

        let req = make_request("ListLogGroups", json!({}));
        let resp = svc.list_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let by_name: std::collections::HashMap<&str, &str> = body["logGroups"]
            .as_array()
            .unwrap()
            .iter()
            .map(|g| {
                (
                    g["logGroupName"].as_str().unwrap(),
                    g["logGroupClass"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(by_name["/infrequent"], "INFREQUENT_ACCESS");
        assert_eq!(by_name["/standard"], "STANDARD");
    }

    #[test]
    fn list_aggregate_log_group_summaries_aggregates_created_groups() {
        let svc = make_service();
        create_group(&svc, "/aws/lambda/fn-a");
        create_group(&svc, "/aws/lambda/fn-b");
        create_group(&svc, "/aws/vpc/flowlogs");
        create_group(&svc, "myapp/web");

        let req = make_request(
            "ListAggregateLogGroupSummaries",
            json!({ "groupBy": "DATA_SOURCE_NAME_AND_TYPE" }),
        );
        let resp = svc.list_aggregate_log_group_summaries(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let summaries = body["aggregateLogGroupSummaries"].as_array().unwrap();

        // Three data sources: /aws/lambda (2), /aws/vpc (1), myapp (1).
        assert_eq!(summaries.len(), 3);
        let total: i64 = summaries
            .iter()
            .map(|s| s["logGroupCount"].as_i64().unwrap())
            .sum();
        assert_eq!(total, 4);

        let find = |name: &str| -> i64 {
            summaries
                .iter()
                .find(|s| {
                    s["groupingIdentifiers"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|id| id["key"] == "dataSource.Name" && id["value"] == name)
                })
                .map(|s| s["logGroupCount"].as_i64().unwrap())
                .unwrap_or(0)
        };
        assert_eq!(find("/aws/lambda"), 2);
        assert_eq!(find("/aws/vpc"), 1);
        assert_eq!(find("myapp"), 1);

        // NAME_AND_TYPE must not emit a Format identifier.
        assert!(!summaries[0]["groupingIdentifiers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| id["key"] == "dataSource.Format"));
    }

    #[test]
    fn list_aggregate_log_group_summaries_format_and_class_filter() {
        let svc = make_service();
        create_group_with_class(&svc, "/aws/lambda/fn", "INFREQUENT_ACCESS");
        create_group(&svc, "/aws/lambda/other"); // STANDARD

        // Filter to INFREQUENT_ACCESS only -> one group.
        let req = make_request(
            "ListAggregateLogGroupSummaries",
            json!({
                "groupBy": "DATA_SOURCE_NAME_TYPE_AND_FORMAT",
                "logGroupClass": "INFREQUENT_ACCESS",
            }),
        );
        let resp = svc.list_aggregate_log_group_summaries(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let summaries = body["aggregateLogGroupSummaries"].as_array().unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0]["logGroupCount"], json!(1));
        // FORMAT groupBy emits a dataSource.Format identifier.
        assert!(summaries[0]["groupingIdentifiers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| id["key"] == "dataSource.Format"));
    }

    #[test]
    fn list_aggregate_log_group_summaries_paginates() {
        let svc = make_service();
        create_group(&svc, "/aws/a/x");
        create_group(&svc, "/aws/b/x");
        create_group(&svc, "/aws/c/x");

        let req = make_request(
            "ListAggregateLogGroupSummaries",
            json!({ "groupBy": "DATA_SOURCE_NAME_AND_TYPE", "limit": 2 }),
        );
        let resp = svc.list_aggregate_log_group_summaries(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            body["aggregateLogGroupSummaries"].as_array().unwrap().len(),
            2
        );
        let token = body["nextToken"].as_str().expect("nextToken on page 1");

        let req2 = make_request(
            "ListAggregateLogGroupSummaries",
            json!({ "groupBy": "DATA_SOURCE_NAME_AND_TYPE", "limit": 2, "nextToken": token }),
        );
        let resp2 = svc.list_aggregate_log_group_summaries(&req2).unwrap();
        let body2: Value = serde_json::from_slice(resp2.body.expect_bytes()).unwrap();
        assert_eq!(
            body2["aggregateLogGroupSummaries"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(body2["nextToken"].is_null());
    }

    #[test]
    fn list_aggregate_log_group_summaries_garbage_token_ends_listing() {
        let svc = make_service();
        create_group(&svc, "/aws/a/x");
        create_group(&svc, "/aws/b/x");

        // A non-integer token must end the listing rather than restart page 1.
        let req = make_request(
            "ListAggregateLogGroupSummaries",
            json!({ "groupBy": "DATA_SOURCE_NAME_AND_TYPE", "nextToken": "not-a-number" }),
        );
        let resp = svc.list_aggregate_log_group_summaries(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            body["aggregateLogGroupSummaries"].as_array().unwrap().len(),
            0
        );
        assert!(body["nextToken"].is_null());
    }
}
