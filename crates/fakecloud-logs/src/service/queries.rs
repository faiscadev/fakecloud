use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::{body_json, require_str, LogsService};
use chrono::Utc;

use crate::query;
use crate::state::{LogEvent, QueryDefinition, QueryInfo};

impl LogsService {
    // ---- Queries ----

    pub(crate) fn start_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName is required",
            )
        })?;
        let start_time = body["startTime"].as_i64().unwrap_or(0);
        let end_time = body["endTime"].as_i64().unwrap_or(0);
        let query_string = body["queryString"].as_str().unwrap_or("").to_string();

        validate_string_length("logGroupName", log_group_name, 1, 512)?;
        validate_optional_string_length("queryString", Some(&query_string), 0, 10000)?;

        let mut state = self.state.write();

        // Verify log group exists
        if !state.log_groups.contains_key(log_group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            ));
        }

        let query_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now().timestamp_millis();

        state.queries.insert(
            query_id.clone(),
            QueryInfo {
                query_id: query_id.clone(),
                log_group_name: log_group_name.to_string(),
                query_string,
                start_time,
                end_time,
                status: "Complete".to_string(),
                create_time: now,
            },
        );

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "queryId": query_id })).unwrap(),
        ))
    }

    pub(crate) fn get_query_results(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let query_id = body["queryId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "queryId is required",
            )
        })?;

        validate_string_length("queryId", query_id, 1, 256)?;

        let state = self.state.read();
        let query_info = state.queries.get(query_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified query does not exist.",
            )
        })?;

        // Parse the query string
        let parsed = query::parse_query(&query_info.query_string);

        // Collect events by stream
        let mut stream_events: Vec<(String, Vec<LogEvent>)> = Vec::new();
        if let Some(group) = state.log_groups.get(&query_info.log_group_name) {
            for stream in group.log_streams.values() {
                stream_events.push((stream.name.clone(), stream.events.clone()));
            }
        }

        let results = query::execute_query(
            &parsed,
            &stream_events,
            query_info.start_time,
            query_info.end_time,
        );

        let records_matched = results.len() as f64;
        let total_scanned: usize = stream_events.iter().map(|(_, e)| e.len()).sum();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "status": query_info.status,
                "results": results,
                "statistics": {
                    "recordsMatched": records_matched,
                    "recordsScanned": total_scanned as f64,
                    "bytesScanned": 0.0,
                },
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn describe_queries(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let log_group_name = body["logGroupName"].as_str();
        let status_filter = body["status"].as_str();

        validate_optional_string_length("logGroupName", log_group_name, 1, 512)?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 1000)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "status",
            &body["status"],
            &[
                "Scheduled",
                "Running",
                "Complete",
                "Failed",
                "Cancelled",
                "Timeout",
                "Unknown",
            ],
        )?;
        validate_optional_enum_value(
            "queryLanguage",
            &body["queryLanguage"],
            &["CWLI", "SQL", "PPL"],
        )?;

        let state = self.state.read();
        let queries: Vec<Value> = state
            .queries
            .values()
            .filter(|q| {
                if let Some(lg) = log_group_name {
                    if q.log_group_name != lg {
                        return false;
                    }
                }
                if let Some(status) = status_filter {
                    if q.status != status {
                        return false;
                    }
                }
                true
            })
            .map(|q| {
                json!({
                    "queryId": q.query_id,
                    "queryString": q.query_string,
                    "status": q.status,
                    "createTime": q.create_time,
                    "logGroupName": q.log_group_name,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "queries": queries })).unwrap(),
        ))
    }

    // ---- Query Definitions ----

    pub(crate) fn put_query_definition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = body["name"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "name is required",
                )
            })?
            .to_string();
        let query_string = body["queryString"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "queryString is required",
                )
            })?
            .to_string();
        let log_group_names: Vec<String> = body["logGroupNames"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let query_definition_id = body["queryDefinitionId"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        validate_string_length("name", &name, 1, 255)?;
        validate_string_length("queryString", &query_string, 1, 10000)?;
        validate_optional_string_length(
            "queryDefinitionId",
            body["queryDefinitionId"].as_str(),
            1,
            256,
        )?;
        validate_optional_string_length("clientToken", body["clientToken"].as_str(), 36, 128)?;
        validate_optional_enum_value(
            "queryLanguage",
            &body["queryLanguage"],
            &["CWLI", "SQL", "PPL"],
        )?;

        let now = Utc::now().timestamp_millis();

        let mut state = self.state.write();
        state.query_definitions.insert(
            query_definition_id.clone(),
            QueryDefinition {
                query_definition_id: query_definition_id.clone(),
                name,
                query_string,
                log_group_names,
                last_modified: now,
            },
        );

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "queryDefinitionId": query_definition_id,
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn describe_query_definitions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name_prefix = body["queryDefinitionNamePrefix"].as_str().unwrap_or("");
        validate_optional_string_length(
            "queryDefinitionNamePrefix",
            body["queryDefinitionNamePrefix"].as_str(),
            1,
            255,
        )?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 1000)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 2048)?;
        validate_optional_enum_value(
            "queryLanguage",
            &body["queryLanguage"],
            &["CWLI", "SQL", "PPL"],
        )?;

        let state = self.state.read();
        let defs: Vec<Value> = state
            .query_definitions
            .values()
            .filter(|qd| name_prefix.is_empty() || qd.name.starts_with(name_prefix))
            .map(|qd| {
                json!({
                    "queryDefinitionId": qd.query_definition_id,
                    "name": qd.name,
                    "queryString": qd.query_string,
                    "logGroupNames": qd.log_group_names,
                    "lastModified": qd.last_modified,
                })
            })
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "queryDefinitions": defs })).unwrap(),
        ))
    }

    pub(crate) fn delete_query_definition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let qd_id = body["queryDefinitionId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "queryDefinitionId is required",
            )
        })?;

        validate_string_length("queryDefinitionId", qd_id, 1, 256)?;

        let mut state = self.state.write();
        let success = state.query_definitions.remove(qd_id).is_some();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "success": success })).unwrap(),
        ))
    }

    pub(crate) fn stop_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let query_id = body["queryId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "queryId is required",
            )
        })?;

        let mut state = self.state.write();
        let query = state.queries.get_mut(query_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("Query {query_id} is not in a cancellable state"),
            )
        })?;

        let was_running = query.status == "Running" || query.status == "Scheduled";
        if was_running {
            query.status = "Cancelled".to_string();
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "success": was_running })).unwrap(),
        ))
    }

    pub(crate) fn list_log_groups_for_query(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let query_id = require_str(&body, "queryId")?;
        validate_string_length("queryId", query_id, 1, 256)?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 50, 500)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        // Stub: return empty log group names
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logGroupIdentifiers": [] })).unwrap(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- describe_query_definitions: queryDefinitionNamePrefix ----

    #[test]
    fn describe_query_definitions_filters_by_name_prefix() {
        let svc = make_service();

        // Create some query definitions
        for name in &["error-queries-1", "error-queries-2", "latency-queries-1"] {
            let req = make_request(
                "PutQueryDefinition",
                json!({
                    "name": name,
                    "queryString": "fields @timestamp | limit 20",
                }),
            );
            svc.put_query_definition(&req).unwrap();
        }

        let req = make_request(
            "DescribeQueryDefinitions",
            json!({ "queryDefinitionNamePrefix": "error" }),
        );
        let resp = svc.describe_query_definitions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let defs = body["queryDefinitions"].as_array().unwrap();
        assert_eq!(defs.len(), 2);
        for d in defs {
            assert!(d["name"].as_str().unwrap().starts_with("error"));
        }
    }

    #[test]
    fn describe_query_definitions_no_prefix_returns_all() {
        let svc = make_service();

        for name in &["a", "b", "c"] {
            let req = make_request(
                "PutQueryDefinition",
                json!({ "name": name, "queryString": "fields @timestamp" }),
            );
            svc.put_query_definition(&req).unwrap();
        }

        let req = make_request("DescribeQueryDefinitions", json!({}));
        let resp = svc.describe_query_definitions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["queryDefinitions"].as_array().unwrap().len(), 3);
    }

    // ---- Query definitions ----

    #[test]
    fn query_definition_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "PutQueryDefinition",
            json!({
                "name": "my-query",
                "queryString": "fields @timestamp, @message | limit 20",
                "logGroupNames": ["/app/web"],
            }),
        );
        let resp = svc.put_query_definition(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let qd_id = body["queryDefinitionId"].as_str().unwrap().to_string();

        // Describe
        let req = make_request("DescribeQueryDefinitions", json!({}));
        let resp = svc.describe_query_definitions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let defs = body["queryDefinitions"].as_array().unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0]["name"], "my-query");
        assert_eq!(defs[0]["logGroupNames"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request(
            "DeleteQueryDefinition",
            json!({ "queryDefinitionId": qd_id }),
        );
        let resp = svc.delete_query_definition(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["success"], true);

        // Verify gone
        let req = make_request("DescribeQueryDefinitions", json!({}));
        let resp = svc.describe_query_definitions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["queryDefinitions"].as_array().unwrap().is_empty());
    }

    #[test]
    fn delete_query_definition_nonexistent_returns_false() {
        let svc = make_service();
        let req = make_request(
            "DeleteQueryDefinition",
            json!({ "queryDefinitionId": "nonexistent-id" }),
        );
        let resp = svc.delete_query_definition(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["success"], false);
    }

    // ---- StopQuery ----

    #[test]
    fn stop_query_nonexistent_fails() {
        let svc = make_service();
        let req = make_request("StopQuery", json!({ "queryId": "nonexistent-query-id" }));
        // StopQuery on a non-running query should still succeed (returns success: false or noop)
        // But a completely nonexistent query depends on implementation
        let result = svc.stop_query(&req);
        // Either it errors or returns success: false — both are valid
        if let Ok(resp) = result {
            let body: Value = serde_json::from_slice(&resp.body).unwrap();
            // success should be false for a non-running query
            assert!(!body["success"].as_bool().unwrap_or(true));
        }
    }
}
