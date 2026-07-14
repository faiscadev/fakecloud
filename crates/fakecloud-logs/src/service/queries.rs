use http::StatusCode;
use serde_json::{json, Value};

use crate::validation::*;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{require_str, LogsService};
use chrono::Utc;

use crate::query;
use crate::state::{LogEvent, QueryDefinition, QueryInfo};

impl LogsService {
    // ---- Queries ----

    pub(crate) fn start_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let log_group_name = body["logGroupName"].as_str();
        let log_group_names: Vec<String> = body["logGroupNames"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let log_group_identifiers: Vec<String> = body["logGroupIdentifiers"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let start_time = body["startTime"].as_i64().unwrap_or(0);
        let end_time = body["endTime"].as_i64().unwrap_or(0);
        let query_string = body["queryString"].as_str().unwrap_or("").to_string();

        // AWS requires exactly one of logGroupName / logGroupNames / logGroupIdentifiers.
        if log_group_name.is_none()
            && log_group_names.is_empty()
            && log_group_identifiers.is_empty()
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupName, logGroupNames or logGroupIdentifiers is required",
            ));
        }
        if let Some(name) = log_group_name {
            validate_string_length("logGroupName", name, 1, 512)?;
        }
        validate_optional_string_length("queryString", Some(&query_string), 0, 10000)?;

        // Reject a malformed query up front (AWS surfaces MalformedQueryException
        // from StartQuery) so a broken query string is never stored to later
        // trip up GetQueryResults.
        if let Err(e) = query::parse_query(&query_string) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedQueryException",
                e,
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Verify single-name shape exists, when provided. The array shapes are
        // accepted as-is; AWS returns results keyed off whichever groups exist.
        if let Some(name) = log_group_name {
            if !state.log_groups.contains_key(name) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "The specified log group does not exist.",
                ));
            }
        }

        let query_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now().timestamp_millis();

        // Capture every group/identifier the query referenced for later
        // `ListLogGroupsForQuery` lookups.
        let mut all_identifiers: Vec<String> = Vec::new();
        if let Some(name) = log_group_name {
            all_identifiers.push(name.to_string());
        }
        all_identifiers.extend(log_group_names.iter().cloned());
        all_identifiers.extend(log_group_identifiers.iter().cloned());

        let primary_name = log_group_name
            .map(String::from)
            .or_else(|| log_group_names.first().cloned())
            .or_else(|| log_group_identifiers.first().cloned())
            .unwrap_or_default();

        state.queries.insert(
            query_id.clone(),
            QueryInfo {
                query_id: query_id.clone(),
                log_group_name: primary_name,
                log_group_identifiers: all_identifiers,
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
        let body = req.json_body();
        let query_id = body["queryId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "queryId is required",
            )
        })?;

        validate_string_length("queryId", query_id, 1, 256)?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let query_info = state.queries.get(query_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "The specified query does not exist.",
            )
        })?;

        // Parse the query string. StartQuery already rejected malformed
        // queries; treat any residual parse failure as an empty pipeline so a
        // stored-then-corrupted query can never panic or return an undeclared
        // error here (GetQueryResults does not declare MalformedQueryException).
        let parsed = query::parse_query(&query_info.query_string).unwrap_or_default();

        // Collect events by stream from every group/identifier the query
        // referenced, not just the legacy single `log_group_name`. ARNs are
        // resolved to bare group names so multi-group StartQuery requests
        // actually scan every requested group. Each event keeps its original
        // index in the stream's full event list so the `@ptr` it produces
        // round-trips through GetLogRecord.
        let mut streams: Vec<query::QueryStream> = Vec::new();
        let mut seen_groups: std::collections::HashSet<String> = Default::default();
        let identifiers: Vec<String> = if query_info.log_group_identifiers.is_empty() {
            vec![query_info.log_group_name.clone()]
        } else {
            query_info.log_group_identifiers.clone()
        };
        for identifier in identifiers {
            let group_name = if identifier.starts_with("arn:") {
                match super::extract_log_group_from_arn(&identifier) {
                    Some(n) => n,
                    None => continue,
                }
            } else {
                identifier
            };
            if !seen_groups.insert(group_name.clone()) {
                continue;
            }
            if let Some(group) = state.log_groups.get(&group_name) {
                let retention_cutoff = group
                    .retention_in_days
                    .map(|d| Utc::now().timestamp_millis() - (d as i64) * 86_400_000);
                for stream in group.log_streams.values() {
                    let events: Vec<(usize, LogEvent)> = stream
                        .events
                        .iter()
                        .enumerate()
                        .filter(|(_, e)| {
                            retention_cutoff.is_none_or(|cutoff| e.timestamp >= cutoff)
                        })
                        .map(|(i, e)| (i, e.clone()))
                        .collect();
                    streams.push(query::QueryStream {
                        group_name: group_name.clone(),
                        stream_name: stream.name.clone(),
                        events,
                    });
                }
            }
        }

        let results = query::execute_query(
            &parsed,
            &streams,
            query_info.start_time,
            query_info.end_time,
        );

        let records_matched = results.len() as f64;
        let total_scanned: usize = streams.iter().map(|s| s.events.len()).sum();

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
        let body = req.json_body();
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

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let (page, next_token) = super::paginate_offset(
            &queries,
            body["maxResults"].as_i64(),
            1000,
            body["nextToken"].as_str(),
        );
        let mut result = json!({ "queries": page });
        if let Some(token) = next_token {
            result["nextToken"] = json!(token);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    // ---- Query Definitions ----

    pub(crate) fn put_query_definition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
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

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let (page, next_token) = super::paginate_offset(
            &defs,
            body["maxResults"].as_i64(),
            1000,
            body["nextToken"].as_str(),
        );
        let mut result = json!({ "queryDefinitions": page });
        if let Some(token) = next_token {
            result["nextToken"] = json!(token);
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    pub(crate) fn delete_query_definition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let qd_id = body["queryDefinitionId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "queryDefinitionId is required",
            )
        })?;

        validate_string_length("queryDefinitionId", qd_id, 1, 256)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let success = state.query_definitions.remove(qd_id).is_some();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "success": success })).unwrap(),
        ))
    }

    pub(crate) fn stop_query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let query_id = body["queryId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "queryId is required",
            )
        })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
        let query_id = require_str(&body, "queryId")?;
        validate_string_length("queryId", query_id, 1, 256)?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 50, 500)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let identifiers = state
            .queries
            .get(query_id)
            .map(|q| q.log_group_identifiers.clone())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "The specified query does not exist.",
                )
            })?;

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logGroupIdentifiers": identifiers })).unwrap(),
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let qd_id = body["queryDefinitionId"].as_str().unwrap().to_string();

        // Describe
        let req = make_request("DescribeQueryDefinitions", json!({}));
        let resp = svc.describe_query_definitions(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["success"], true);

        // Verify gone
        let req = make_request("DescribeQueryDefinitions", json!({}));
        let resp = svc.describe_query_definitions(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
            let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
            // success should be false for a non-running query
            assert!(!body["success"].as_bool().unwrap_or(true));
        }
    }

    // ---- start_query error paths ----

    #[test]
    fn start_query_missing_log_group_name_errors() {
        let svc = make_service();
        let req = make_request("StartQuery", json!({ "queryString": "fields @timestamp" }));
        assert!(svc.start_query(&req).is_err());
    }

    #[test]
    fn start_query_unknown_log_group_errors() {
        let svc = make_service();
        let req = make_request(
            "StartQuery",
            json!({
                "logGroupName": "missing",
                "startTime": 0,
                "endTime": 0,
                "queryString": "fields @timestamp"
            }),
        );
        assert!(svc.start_query(&req).is_err());
    }

    #[test]
    fn start_query_malformed_query_string_errors() {
        let svc = make_service();
        create_group(&svc, "app");
        // A lone regex delimiter previously panicked when GetQueryResults parsed
        // it; StartQuery now rejects it with MalformedQueryException.
        let req = make_request(
            "StartQuery",
            json!({
                "logGroupName": "app",
                "startTime": 0,
                "endTime": 0,
                "queryString": "filter @message like /"
            }),
        );
        match svc.start_query(&req) {
            Err(e) => assert_eq!(e.code(), "MalformedQueryException"),
            Ok(_) => panic!("expected MalformedQueryException"),
        }
    }

    #[test]
    fn get_query_results_does_not_panic_on_bad_stored_query() {
        // Even if a malformed query slips into storage, GetQueryResults must
        // return (empty) results rather than panic.
        let svc = make_service();
        create_group(&svc, "app");
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_or_create("123456789012");
            state.queries.insert(
                "q-bad".to_string(),
                crate::state::QueryInfo {
                    query_id: "q-bad".to_string(),
                    log_group_name: "app".to_string(),
                    log_group_identifiers: vec!["app".to_string()],
                    query_string: "filter x = \"".to_string(),
                    start_time: 0,
                    end_time: 0,
                    status: "Complete".to_string(),
                    create_time: 0,
                },
            );
        }
        let get = make_request("GetQueryResults", json!({"queryId": "q-bad"}));
        let resp = svc.get_query_results(&get).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["results"].is_array());
    }

    #[test]
    fn start_query_ok_path() {
        let svc = make_service();
        create_group(&svc, "app");
        let req = make_request(
            "StartQuery",
            json!({
                "logGroupName": "app",
                "startTime": 0,
                "endTime": 0,
                "queryString": "fields @timestamp"
            }),
        );
        let resp = svc.start_query(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["queryId"].is_string());
    }

    // ---- get_query_results error paths ----

    #[test]
    fn get_query_results_missing_query_id_errors() {
        let svc = make_service();
        let req = make_request("GetQueryResults", json!({}));
        assert!(svc.get_query_results(&req).is_err());
    }

    #[test]
    fn get_query_results_unknown_id_errors() {
        let svc = make_service();
        let req = make_request("GetQueryResults", json!({"queryId": "missing"}));
        assert!(svc.get_query_results(&req).is_err());
    }

    #[test]
    fn get_query_results_returns_complete_status() {
        let svc = make_service();
        create_group(&svc, "app");
        let start = make_request(
            "StartQuery",
            json!({
                "logGroupName": "app",
                "startTime": 0,
                "endTime": 0,
                "queryString": "fields @timestamp"
            }),
        );
        let resp = svc.start_query(&start).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let qid = body["queryId"].as_str().unwrap().to_string();
        let get = make_request("GetQueryResults", json!({"queryId": qid}));
        let resp = svc.get_query_results(&get).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["status"], "Complete");
        assert!(body["results"].is_array());
    }

    // ---- put_query_definition error paths ----

    #[test]
    fn put_query_definition_update_existing() {
        let svc = make_service();
        let put = make_request(
            "PutQueryDefinition",
            json!({"name": "qd-1", "queryString": "fields @timestamp"}),
        );
        let resp = svc.put_query_definition(&put).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let qid = body["queryDefinitionId"].as_str().unwrap().to_string();

        // Update using queryDefinitionId
        let update = make_request(
            "PutQueryDefinition",
            json!({
                "queryDefinitionId": qid,
                "name": "qd-1-updated",
                "queryString": "fields @timestamp | limit 10"
            }),
        );
        svc.put_query_definition(&update).unwrap();

        let list = make_request("DescribeQueryDefinitions", json!({}));
        let resp = svc.describe_query_definitions(&list).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let defs = body["queryDefinitions"].as_array().unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0]["name"], "qd-1-updated");
    }

    #[test]
    fn describe_queries_filters_by_log_group() {
        let svc = make_service();
        create_group(&svc, "app-a");
        create_group(&svc, "app-b");
        for g in &["app-a", "app-b"] {
            let req = make_request(
                "StartQuery",
                json!({
                    "logGroupName": g,
                    "startTime": 0,
                    "endTime": 0,
                    "queryString": "fields @timestamp"
                }),
            );
            svc.start_query(&req).unwrap();
        }
        let req = make_request("DescribeQueries", json!({"logGroupName": "app-a"}));
        let resp = svc.describe_queries(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let queries = body["queries"].as_array().unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0]["logGroupName"], "app-a");
    }

    #[test]
    fn describe_queries_returns_all_when_no_filter() {
        let svc = make_service();
        create_group(&svc, "g1");
        let req = make_request(
            "StartQuery",
            json!({
                "logGroupName": "g1",
                "startTime": 0,
                "endTime": 0,
                "queryString": "fields @timestamp"
            }),
        );
        svc.start_query(&req).unwrap();
        let req = make_request("DescribeQueries", json!({}));
        let resp = svc.describe_queries(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["queries"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn list_log_groups_for_query_returns_started_groups() {
        let svc = make_service();
        create_group(&svc, "app");
        let start = make_request(
            "StartQuery",
            json!({
                "logGroupName": "app",
                "startTime": 0,
                "endTime": 0,
                "queryString": "fields @timestamp"
            }),
        );
        let resp = svc.start_query(&start).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let qid = body["queryId"].as_str().unwrap().to_string();
        let req = make_request("ListLogGroupsForQuery", json!({"queryId": qid}));
        let resp = svc.list_log_groups_for_query(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let ids = body["logGroupIdentifiers"].as_array().unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].as_str().unwrap(), "app");
    }

    #[test]
    fn list_log_groups_for_query_returns_array_form_groups() {
        let svc = make_service();
        let start = make_request(
            "StartQuery",
            json!({
                "logGroupIdentifiers": [
                    "arn:aws:logs:us-east-1:123456789012:log-group:a:*",
                    "arn:aws:logs:us-east-1:123456789012:log-group:b:*"
                ],
                "startTime": 0,
                "endTime": 0,
                "queryString": "fields @timestamp"
            }),
        );
        let resp = svc.start_query(&start).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let qid = body["queryId"].as_str().unwrap().to_string();
        let req = make_request("ListLogGroupsForQuery", json!({"queryId": qid}));
        let resp = svc.list_log_groups_for_query(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["logGroupIdentifiers"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn list_log_groups_for_query_missing_id_errors() {
        let svc = make_service();
        let req = make_request("ListLogGroupsForQuery", json!({}));
        assert!(svc.list_log_groups_for_query(&req).is_err());
    }
}
