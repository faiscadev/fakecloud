use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::{body_json, require_str, LogsService};
use chrono::Utc;

use crate::state::{ImportTask, Integration, LookupTable, ScheduledQuery};

impl LogsService {
    pub(crate) fn create_import_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let import_source_arn = require_str(&body, "importSourceArn")?;
        let import_role_arn = require_str(&body, "importRoleArn")?;
        validate_string_length("importRoleArn", import_role_arn, 1, 2048)?;
        let log_group_name = body["logGroupName"].as_str().map(|s| s.to_string());

        let import_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now().timestamp_millis();

        let task = ImportTask {
            import_id: import_id.clone(),
            import_source_arn: import_source_arn.to_string(),
            import_role_arn: import_role_arn.to_string(),
            log_group_name,
            status: "RUNNING".to_string(),
            creation_time: now,
        };

        let mut state = self.state.write();
        state.import_tasks.insert(import_id.clone(), task);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "importId": import_id })).unwrap(),
        ))
    }

    pub(crate) fn describe_import_tasks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length("importId", body["importId"].as_str(), 1, 256)?;
        validate_optional_enum_value(
            "importStatus",
            &body["importStatus"],
            &["IN_PROGRESS", "CANCELLED", "COMPLETED", "FAILED"],
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;

        let state = self.state.read();
        let tasks: Vec<Value> = state
            .import_tasks
            .values()
            .map(|t| {
                json!({
                    "importId": t.import_id,
                    "importSourceArn": t.import_source_arn,
                    "importStatus": t.status,
                    "creationTime": t.creation_time,
                })
            })
            .collect();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "imports": tasks })).unwrap(),
        ))
    }

    pub(crate) fn describe_import_task_batches(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let import_id = require_str(&body, "importId")?;
        validate_string_length("importId", import_id, 1, 256)?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        // Stub: return empty batches
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "importBatches": [] })).unwrap(),
        ))
    }

    pub(crate) fn cancel_import_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let import_id = require_str(&body, "importId")?;

        let mut state = self.state.write();
        match state.import_tasks.get_mut(import_id) {
            Some(task) => {
                task.status = "CANCELLED".to_string();
                Ok(AwsResponse::json(StatusCode::OK, "{}"))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Import task not found: {import_id}"),
            )),
        }
    }

    // -- Integrations --

    pub(crate) fn put_integration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("resourceConfig", &body["resourceConfig"])?;
        let integration_name = require_str(&body, "integrationName")?;
        validate_string_length("integrationName", integration_name, 1, 50)?;
        let integration_type = require_str(&body, "integrationType")?;
        validate_enum("integrationType", integration_type, &["OPENSEARCH"])?;
        let resource_config = body["resourceConfig"].clone();

        let now = Utc::now().timestamp_millis();
        let integration = Integration {
            integration_name: integration_name.to_string(),
            integration_type: integration_type.to_string(),
            resource_config,
            status: "ACTIVE".to_string(),
            creation_time: now,
        };

        let mut state = self.state.write();
        state
            .integrations
            .insert(integration_name.to_string(), integration);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "integrationName": integration_name,
                "integrationStatus": "ACTIVE"
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn get_integration(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let integration_name = require_str(&body, "integrationName")?;

        let state = self.state.read();
        match state.integrations.get(integration_name) {
            Some(i) => Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({
                    "integrationName": i.integration_name,
                    "integrationType": i.integration_type,
                    "integrationStatus": i.status,
                }))
                .unwrap(),
            )),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Integration not found: {integration_name}"),
            )),
        }
    }

    pub(crate) fn delete_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let integration_name = require_str(&body, "integrationName")?;
        validate_string_length("integrationName", integration_name, 1, 50)?;

        let mut state = self.state.write();
        state.integrations.remove(integration_name);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn list_integrations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length(
            "integrationNamePrefix",
            body["integrationNamePrefix"].as_str(),
            1,
            50,
        )?;
        validate_optional_enum_value("integrationType", &body["integrationType"], &["OPENSEARCH"])?;
        validate_optional_enum_value(
            "integrationStatus",
            &body["integrationStatus"],
            &["PROVISIONING", "ACTIVE", "FAILED"],
        )?;

        let state = self.state.read();
        let integrations: Vec<Value> = state
            .integrations
            .values()
            .map(|i| {
                json!({
                    "integrationName": i.integration_name,
                    "integrationType": i.integration_type,
                    "integrationStatus": i.status,
                })
            })
            .collect();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "integrationSummaries": integrations })).unwrap(),
        ))
    }

    // -- Lookup tables --

    pub(crate) fn create_lookup_table(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let lookup_table_name = require_str(&body, "lookupTableName")?;
        validate_string_length("lookupTableName", lookup_table_name, 1, 256)?;
        let table_body = require_str(&body, "tableBody")?;
        validate_string_length("tableBody", table_body, 1, 10485760)?;
        validate_optional_string_length("description", body["description"].as_str(), 0, 1024)?;
        validate_optional_string_length("kmsKeyId", body["kmsKeyId"].as_str(), 0, 256)?;

        let state_r = self.state.read();
        let account_id = state_r.account_id.clone();
        let region = state_r.region.clone();
        drop(state_r);

        let arn = format!("arn:aws:logs:{region}:{account_id}:lookup-table:{lookup_table_name}");
        let now = Utc::now().timestamp_millis();

        let table = LookupTable {
            lookup_table_name: lookup_table_name.to_string(),
            arn: arn.clone(),
            table_body: table_body.to_string(),
            creation_time: now,
            last_modified_time: now,
        };

        let mut state = self.state.write();
        state.lookup_tables.insert(arn.clone(), table);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "lookupTableArn": arn })).unwrap(),
        ))
    }

    pub(crate) fn get_lookup_table(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let lookup_table_arn = require_str(&body, "lookupTableArn")?;

        let state = self.state.read();
        match state.lookup_tables.get(lookup_table_arn) {
            Some(t) => Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({
                    "lookupTableName": t.lookup_table_name,
                    "lookupTableArn": t.arn,
                    "tableBody": t.table_body,
                    "creationTime": t.creation_time,
                    "lastModifiedTime": t.last_modified_time,
                }))
                .unwrap(),
            )),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Lookup table not found: {lookup_table_arn}"),
            )),
        }
    }

    pub(crate) fn describe_lookup_tables(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_string_length(
            "lookupTableNamePrefix",
            body["lookupTableNamePrefix"].as_str(),
            1,
            256,
        )?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 100)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;

        let state = self.state.read();
        let tables: Vec<Value> = state
            .lookup_tables
            .values()
            .map(|t| {
                json!({
                    "lookupTableName": t.lookup_table_name,
                    "lookupTableArn": t.arn,
                })
            })
            .collect();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "lookupTables": tables })).unwrap(),
        ))
    }

    pub(crate) fn delete_lookup_table(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let lookup_table_arn = require_str(&body, "lookupTableArn")?;

        let mut state = self.state.write();
        state.lookup_tables.remove(lookup_table_arn);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn update_lookup_table(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let lookup_table_arn = require_str(&body, "lookupTableArn")?;
        let table_body = require_str(&body, "tableBody")?;

        let mut state = self.state.write();
        match state.lookup_tables.get_mut(lookup_table_arn) {
            Some(t) => {
                t.table_body = table_body.to_string();
                t.last_modified_time = Utc::now().timestamp_millis();
                Ok(AwsResponse::json(StatusCode::OK, "{}"))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Lookup table not found: {lookup_table_arn}"),
            )),
        }
    }

    // -- Scheduled queries --

    pub(crate) fn create_scheduled_query(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let name = require_str(&body, "name")?;
        validate_string_length("name", name, 1, 255)?;
        validate_optional_string_length("description", body["description"].as_str(), 0, 1024)?;
        let query_string = require_str(&body, "queryString")?;
        validate_string_length("queryString", query_string, 0, 10000)?;
        let query_language = require_str(&body, "queryLanguage")?;
        validate_enum("queryLanguage", query_language, &["CWLI", "SQL", "PPL"])?;
        let schedule_expression = require_str(&body, "scheduleExpression")?;
        validate_string_length("scheduleExpression", schedule_expression, 0, 256)?;
        let execution_role_arn = require_str(&body, "executionRoleArn")?;
        validate_string_length("executionRoleArn", execution_role_arn, 1, 2048)?;
        validate_optional_string_length("timezone", body["timezone"].as_str(), 1, 2048)?;
        validate_optional_range_i64(
            "scheduleStartTime",
            body["scheduleStartTime"].as_i64(),
            0,
            i64::MAX,
        )?;
        validate_optional_range_i64(
            "scheduleEndTime",
            body["scheduleEndTime"].as_i64(),
            0,
            i64::MAX,
        )?;
        validate_optional_enum_value("state", &body["state"], &["ENABLED", "DISABLED"])?;

        let state_r = self.state.read();
        let account_id = state_r.account_id.clone();
        let region = state_r.region.clone();
        drop(state_r);

        let arn = format!("arn:aws:logs:{region}:{account_id}:scheduled-query:{name}");
        let now = Utc::now().timestamp_millis();

        let sq = ScheduledQuery {
            name: name.to_string(),
            arn: arn.clone(),
            query_string: query_string.to_string(),
            query_language: query_language.to_string(),
            schedule_expression: schedule_expression.to_string(),
            execution_role_arn: execution_role_arn.to_string(),
            status: "ACTIVE".to_string(),
            creation_time: now,
            last_modified_time: now,
        };

        let mut state = self.state.write();
        state.scheduled_queries.insert(arn.clone(), sq);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "scheduledQueryArn": arn })).unwrap(),
        ))
    }

    pub(crate) fn get_scheduled_query(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let identifier = require_str(&body, "identifier")?;

        let state = self.state.read();
        match state.scheduled_queries.get(identifier) {
            Some(sq) => Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({
                    "scheduledQueryArn": sq.arn,
                    "name": sq.name,
                    "queryString": sq.query_string,
                    "queryLanguage": sq.query_language,
                    "scheduleExpression": sq.schedule_expression,
                    "executionRoleArn": sq.execution_role_arn,
                }))
                .unwrap(),
            )),
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Scheduled query not found: {identifier}"),
            )),
        }
    }

    pub(crate) fn get_scheduled_query_history(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let _identifier = require_str(&body, "identifier")?;
        validate_required("startTime", &body["startTime"])?;
        validate_required("endTime", &body["endTime"])?;
        validate_optional_range_i64("startTime", body["startTime"].as_i64(), 0, i64::MAX)?;
        validate_optional_range_i64("endTime", body["endTime"].as_i64(), 0, i64::MAX)?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 1000)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        // Stub: return empty history
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "triggerHistory": [] })).unwrap(),
        ))
    }

    pub(crate) fn list_scheduled_queries(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 1000)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        validate_optional_enum_value("state", &body["state"], &["ENABLED", "DISABLED"])?;

        let state = self.state.read();
        let queries: Vec<Value> = state
            .scheduled_queries
            .values()
            .map(|sq| {
                json!({
                    "name": sq.name,
                    "scheduledQueryArn": sq.arn,
                })
            })
            .collect();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "scheduledQueries": queries })).unwrap(),
        ))
    }

    pub(crate) fn delete_scheduled_query(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let identifier = require_str(&body, "identifier")?;

        let mut state = self.state.write();
        state.scheduled_queries.remove(identifier);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn update_scheduled_query(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let identifier = require_str(&body, "identifier")?;
        let query_string = require_str(&body, "queryString")?;
        let query_language = require_str(&body, "queryLanguage")?;
        let schedule_expression = require_str(&body, "scheduleExpression")?;
        let execution_role_arn = require_str(&body, "executionRoleArn")?;

        let mut state = self.state.write();
        match state.scheduled_queries.get_mut(identifier) {
            Some(sq) => {
                sq.query_string = query_string.to_string();
                sq.query_language = query_language.to_string();
                sq.schedule_expression = schedule_expression.to_string();
                sq.execution_role_arn = execution_role_arn.to_string();
                sq.last_modified_time = Utc::now().timestamp_millis();
                Ok(AwsResponse::json(StatusCode::OK, "{}"))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Scheduled query not found: {identifier}"),
            )),
        }
    }

    // -- Misc stubs --

    pub(crate) fn start_live_tail(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("logGroupIdentifiers", &body["logGroupIdentifiers"])?;
        validate_optional_string_length(
            "logEventFilterPattern",
            body["logEventFilterPattern"].as_str(),
            0,
            1024,
        )?;
        let session_id = uuid::Uuid::new_v4().to_string();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "responseStream": {
                    "sessionStart": {
                        "sessionId": session_id,
                        "logGroupIdentifiers": [],
                    }
                }
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn put_bearer_token_authentication(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required(
            "bearerTokenAuthenticationEnabled",
            &body["bearerTokenAuthenticationEnabled"],
        )?;
        let log_group_identifier = require_str(&body, "logGroupIdentifier")?;
        validate_string_length("logGroupIdentifier", log_group_identifier, 1, 2048)?;
        let enabled = body["bearerTokenAuthenticationEnabled"]
            .as_bool()
            .unwrap_or(false);

        let mut state = self.state.write();
        state
            .bearer_token_auth
            .insert(log_group_identifier.to_string(), enabled);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn get_log_object(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("logObjectPointer", &body["logObjectPointer"])?;
        validate_optional_string_length(
            "logObjectPointer",
            body["logObjectPointer"].as_str(),
            1,
            512,
        )?;
        // Stub: return empty (fieldStream is streaming, represented as empty object)
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn get_log_fields(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("dataSourceName", &body["dataSourceName"])?;
        validate_required("dataSourceType", &body["dataSourceType"])?;
        // Stub: return empty log fields
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logFields": [] })).unwrap(),
        ))
    }

    pub(crate) fn associate_source_to_s3_table_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        validate_required("dataSource", &body["dataSource"])?;
        let integration_arn = require_str(&body, "integrationArn")?;
        let data_source = body["dataSource"].clone();
        let source_id = data_source
            .as_object()
            .and_then(|o| o.get("resourceArn"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let mut state = self.state.write();
        state
            .s3_table_sources
            .entry(integration_arn.to_string())
            .or_default()
            .push(source_id);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn list_sources_for_s3_table_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let integration_arn = require_str(&body, "integrationArn")?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 100)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;

        let state = self.state.read();
        let sources: Vec<Value> = state
            .s3_table_sources
            .get(integration_arn)
            .map(|sources| {
                sources
                    .iter()
                    .map(|s| {
                        json!({
                            "identifier": s,
                            "status": "ACTIVE",
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "sources": sources })).unwrap(),
        ))
    }

    pub(crate) fn disassociate_source_from_s3_table_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body_json(req);
        let identifier = require_str(&body, "identifier")?;
        validate_string_length("identifier", identifier, 1, 2048)?;
        // No-op stub (we don't track detailed enough to remove specific sources)
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }
}

#[cfg(test)]
mod tests {
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- Misc operations ----

    #[test]
    fn get_log_group_fields_returns_stub() {
        let svc = make_service();
        create_group(&svc, "fields-group");

        let req = make_request(
            "GetLogGroupFields",
            json!({ "logGroupName": "fields-group" }),
        );
        let resp = svc.get_log_group_fields(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["logGroupFields"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_metric_filter_matches() {
        let svc = make_service();

        let req = make_request(
            "TestMetricFilter",
            json!({
                "filterPattern": "ERROR",
                "logEventMessages": ["ERROR: oops", "INFO: ok", "ERROR: again"],
            }),
        );
        let resp = svc.test_metric_filter(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["matches"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn stop_query_marks_as_cancelled() {
        let svc = make_service();
        create_group(&svc, "sq-group");

        let req = make_request(
            "StartQuery",
            json!({
                "logGroupName": "sq-group",
                "startTime": 0,
                "endTime": 9999999999i64,
                "queryString": "fields @timestamp",
            }),
        );
        let resp = svc.start_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let qid = body["queryId"].as_str().unwrap().to_string();

        // Manually set query status to Running so we can test cancellation
        {
            let mut state = svc.state.write();
            state.queries.get_mut(&qid).unwrap().status = "Running".to_string();
        }

        let req = make_request("StopQuery", json!({ "queryId": &qid }));
        let resp = svc.stop_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["success"], true);

        let state = svc.state.read();
        assert_eq!(state.queries[&qid].status, "Cancelled");
    }

    #[test]
    fn put_log_group_deletion_protection() {
        let svc = make_service();
        create_group(&svc, "prot-group");

        let req = make_request(
            "PutLogGroupDeletionProtection",
            json!({
                "logGroupIdentifier": "prot-group",
                "deletionProtectionEnabled": true,
            }),
        );
        svc.put_log_group_deletion_protection(&req).unwrap();

        let state = svc.state.read();
        assert!(state.log_groups["prot-group"].deletion_protection);
    }

    #[test]
    fn get_log_record_returns_empty_stub() {
        let svc = make_service();

        let req = make_request(
            "GetLogRecord",
            json!({ "logRecordPointer": "some-pointer" }),
        );
        let resp = svc.get_log_record(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["logRecord"].is_object());
    }

    #[test]
    fn list_anomalies_returns_empty() {
        let svc = make_service();

        let req = make_request("ListAnomalies", json!({}));
        let resp = svc.list_anomalies(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["anomalies"].as_array().unwrap().is_empty());
    }

    #[test]
    fn update_anomaly_noop() {
        let svc = make_service();
        let req = make_request(
            "UpdateAnomaly",
            json!({"anomalyDetectorArn": "arn:aws:logs:us-east-1:123456789012:anomaly-detector:test"}),
        );
        svc.update_anomaly(&req).unwrap();
    }

    // -- Import tasks --

    #[test]
    fn import_task_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "CreateImportTask",
            json!({
                "importSourceArn": "arn:aws:s3:::my-bucket/logs",
                "importRoleArn": "arn:aws:iam::123456789012:role/import-role"
            }),
        );
        let resp = svc.create_import_task(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let import_id = body["importId"].as_str().unwrap().to_string();

        let req = make_request("DescribeImportTasks", json!({}));
        let resp = svc.describe_import_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["imports"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DescribeImportTaskBatches",
            json!({ "importId": import_id }),
        );
        let resp = svc.describe_import_task_batches(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["importBatches"].as_array().unwrap().is_empty());

        let req = make_request("CancelImportTask", json!({ "importId": import_id }));
        svc.cancel_import_task(&req).unwrap();

        let req = make_request("DescribeImportTasks", json!({}));
        let resp = svc.describe_import_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["imports"][0]["importStatus"].as_str().unwrap(),
            "CANCELLED"
        );
    }

    // -- Integrations --

    #[test]
    fn integration_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "PutIntegration",
            json!({
                "integrationName": "test-int",
                "integrationType": "OPENSEARCH",
                "resourceConfig": { "openSearchResourceConfig": {} }
            }),
        );
        svc.put_integration(&req).unwrap();

        let req = make_request("GetIntegration", json!({ "integrationName": "test-int" }));
        let resp = svc.get_integration(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["integrationName"].as_str().unwrap(), "test-int");

        let req = make_request("ListIntegrations", json!({}));
        let resp = svc.list_integrations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["integrationSummaries"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DeleteIntegration",
            json!({ "integrationName": "test-int" }),
        );
        svc.delete_integration(&req).unwrap();

        let req = make_request("ListIntegrations", json!({}));
        let resp = svc.list_integrations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["integrationSummaries"].as_array().unwrap().is_empty());
    }

    // -- Lookup tables --

    #[test]
    fn lookup_table_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "CreateLookupTable",
            json!({
                "lookupTableName": "test-table",
                "tableBody": "key,value\na,b"
            }),
        );
        let resp = svc.create_lookup_table(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let arn = body["lookupTableArn"].as_str().unwrap().to_string();

        let req = make_request("GetLookupTable", json!({ "lookupTableArn": arn }));
        let resp = svc.get_lookup_table(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["lookupTableName"].as_str().unwrap(), "test-table");

        let req = make_request("DescribeLookupTables", json!({}));
        let resp = svc.describe_lookup_tables(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["lookupTables"].as_array().unwrap().len(), 1);

        let req = make_request(
            "UpdateLookupTable",
            json!({ "lookupTableArn": arn, "tableBody": "key,value\nc,d" }),
        );
        svc.update_lookup_table(&req).unwrap();

        let req = make_request("DeleteLookupTable", json!({ "lookupTableArn": arn }));
        svc.delete_lookup_table(&req).unwrap();

        let req = make_request("DescribeLookupTables", json!({}));
        let resp = svc.describe_lookup_tables(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["lookupTables"].as_array().unwrap().is_empty());
    }

    // -- Scheduled queries --

    #[test]
    fn scheduled_query_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "CreateScheduledQuery",
            json!({
                "name": "test-sq",
                "queryString": "fields @timestamp | limit 10",
                "queryLanguage": "CWLI",
                "scheduleExpression": "rate(1 hour)",
                "executionRoleArn": "arn:aws:iam::123456789012:role/exec"
            }),
        );
        let resp = svc.create_scheduled_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let arn = body["scheduledQueryArn"].as_str().unwrap().to_string();

        let req = make_request("GetScheduledQuery", json!({ "identifier": arn }));
        let resp = svc.get_scheduled_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["name"].as_str().unwrap(), "test-sq");

        let req = make_request(
            "GetScheduledQueryHistory",
            json!({ "identifier": arn, "startTime": 0_i64, "endTime": 9999999999_i64 }),
        );
        let resp = svc.get_scheduled_query_history(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["triggerHistory"].as_array().unwrap().is_empty());

        let req = make_request("ListScheduledQueries", json!({}));
        let resp = svc.list_scheduled_queries(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["scheduledQueries"].as_array().unwrap().len(), 1);

        let req = make_request(
            "UpdateScheduledQuery",
            json!({
                "identifier": arn,
                "queryString": "fields @message | limit 5",
                "queryLanguage": "CWLI",
                "scheduleExpression": "rate(2 hours)",
                "executionRoleArn": "arn:aws:iam::123456789012:role/exec"
            }),
        );
        svc.update_scheduled_query(&req).unwrap();

        let req = make_request("DeleteScheduledQuery", json!({ "identifier": arn }));
        svc.delete_scheduled_query(&req).unwrap();

        let req = make_request("ListScheduledQueries", json!({}));
        let resp = svc.list_scheduled_queries(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["scheduledQueries"].as_array().unwrap().is_empty());
    }

    // -- Misc stubs --

    #[test]
    fn start_live_tail_returns_session() {
        let svc = make_service();
        let req = make_request(
            "StartLiveTail",
            json!({ "logGroupIdentifiers": ["/test/group"] }),
        );
        let resp = svc.start_live_tail(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["responseStream"]["sessionStart"]["sessionId"]
            .as_str()
            .is_some());
    }

    #[test]
    fn list_log_groups_delegates_to_describe() {
        let svc = make_service();
        create_group(&svc, "/test/list");
        let req = make_request("DescribeLogGroups", json!({}));
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["logGroups"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn list_log_groups_for_query_returns_empty() {
        let svc = make_service();
        let req = make_request(
            "ListLogGroupsForQuery",
            json!({ "queryId": "some-query-id" }),
        );
        let resp = svc.list_log_groups_for_query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["logGroupIdentifiers"].as_array().unwrap().is_empty());
    }

    #[test]
    fn list_aggregate_log_group_summaries_returns_empty() {
        let svc = make_service();
        let req = make_request(
            "ListAggregateLogGroupSummaries",
            json!({ "groupBy": "DATA_SOURCE_NAME_AND_TYPE" }),
        );
        let resp = svc.list_aggregate_log_group_summaries(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["aggregateLogGroupSummaries"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn put_bearer_token_authentication_stores_flag() {
        let svc = make_service();
        create_group(&svc, "/test/bearer");
        let req = make_request(
            "PutBearerTokenAuthentication",
            json!({
                "logGroupIdentifier": "/test/bearer",
                "bearerTokenAuthenticationEnabled": true
            }),
        );
        svc.put_bearer_token_authentication(&req).unwrap();
    }

    #[test]
    fn get_log_object_returns_stub() {
        let svc = make_service();
        let req = make_request(
            "GetLogObject",
            json!({ "logObjectPointer": "some-pointer" }),
        );
        let resp = svc.get_log_object(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body.is_object());
    }

    #[test]
    fn get_log_fields_returns_stub() {
        let svc = make_service();
        let req = make_request(
            "GetLogFields",
            json!({ "dataSourceName": "test", "dataSourceType": "CW_LOG" }),
        );
        let resp = svc.get_log_fields(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["logFields"].as_array().unwrap().is_empty());
    }

    #[test]
    fn s3_table_integration_stubs() {
        let svc = make_service();

        let req = make_request(
            "AssociateSourceToS3TableIntegration",
            json!({
                "integrationArn": "arn:aws:logs:us-east-1:123456789012:integration:test",
                "dataSource": { "resourceArn": "arn:aws:logs:us-east-1:123456789012:log-group:test" }
            }),
        );
        svc.associate_source_to_s3_table_integration(&req).unwrap();

        let req = make_request(
            "ListSourcesForS3TableIntegration",
            json!({
                "integrationArn": "arn:aws:logs:us-east-1:123456789012:integration:test"
            }),
        );
        let resp = svc.list_sources_for_s3_table_integration(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["sources"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DisassociateSourceFromS3TableIntegration",
            json!({ "identifier": "arn:aws:logs:us-east-1:123456789012:integration:test" }),
        );
        svc.disassociate_source_from_s3_table_integration(&req)
            .unwrap();
    }

    #[test]
    fn update_delivery_configuration_noop() {
        let svc = make_service();
        // First create a delivery setup
        let req = make_request(
            "PutDeliverySource",
            json!({
                "name": "test-ds",
                "resourceArn": "arn:aws:logs:us-east-1:123456789012:log-group:dummy",
                "logType": "APPLICATION_LOGS"
            }),
        );
        svc.put_delivery_source(&req).unwrap();

        let req = make_request(
            "PutDeliveryDestination",
            json!({
                "name": "test-dd",
                "deliveryDestinationConfiguration": {
                    "destinationResourceArn": "arn:aws:s3:::test-bucket"
                }
            }),
        );
        svc.put_delivery_destination(&req).unwrap();

        let req = make_request(
            "CreateDelivery",
            json!({
                "deliverySourceName": "test-ds",
                "deliveryDestinationArn": "arn:aws:logs:us-east-1:123456789012:delivery-destination:test-dd"
            }),
        );
        let resp = svc.create_delivery(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let delivery_id = body["delivery"]["id"].as_str().unwrap().to_string();

        let req = make_request("UpdateDeliveryConfiguration", json!({ "id": delivery_id }));
        svc.update_delivery_configuration(&req).unwrap();
    }

    #[test]
    fn describe_configuration_templates_returns_empty() {
        let svc = make_service();
        let req = make_request("DescribeConfigurationTemplates", json!({}));
        let resp = svc.describe_configuration_templates(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["configurationTemplates"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    // ---- GetLogGroupFields ----

    #[test]
    fn get_log_group_fields_nonexistent_group_errors() {
        let svc = make_service();
        let req = make_request(
            "GetLogGroupFields",
            json!({ "logGroupName": "nonexistent" }),
        );
        assert!(svc.get_log_group_fields(&req).is_err());
    }

    // ---- PutLogGroupDeletionProtection ----

    #[test]
    fn deletion_protection_toggle() {
        let svc = make_service();
        create_group(&svc, "dp-toggle");

        // Enable
        let req = make_request(
            "PutLogGroupDeletionProtection",
            json!({
                "logGroupIdentifier": "dp-toggle",
                "deletionProtectionEnabled": true,
            }),
        );
        svc.put_log_group_deletion_protection(&req).unwrap();

        let state = svc.state.read();
        assert!(state.log_groups["dp-toggle"].deletion_protection);
        drop(state);

        // Disable
        let req = make_request(
            "PutLogGroupDeletionProtection",
            json!({
                "logGroupIdentifier": "dp-toggle",
                "deletionProtectionEnabled": false,
            }),
        );
        svc.put_log_group_deletion_protection(&req).unwrap();

        let state = svc.state.read();
        assert!(!state.log_groups["dp-toggle"].deletion_protection);
    }

    // ---- GetLogRecord ----

    #[test]
    fn get_log_record_returns_object() {
        let svc = make_service();
        let req = make_request(
            "GetLogRecord",
            json!({ "logRecordPointer": "any-pointer-value" }),
        );
        let resp = svc.get_log_record(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["logRecord"].is_object());
    }
}
