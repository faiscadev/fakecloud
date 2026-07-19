use http::StatusCode;
use serde_json::{json, Value};

use crate::validation::*;
use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{require_str, LogsService};
use chrono::Utc;

use crate::state::{ImportTask, Integration, LookupTable, ScheduledQuery};

impl LogsService {
    pub(crate) fn create_import_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
        validate_optional_string_length("importId", body["importId"].as_str(), 1, 256)?;
        validate_optional_enum_value(
            "importStatus",
            &body["importStatus"],
            &["IN_PROGRESS", "CANCELLED", "COMPLETED", "FAILED"],
        )?;
        validate_optional_range_i64("limit", body["limit"].as_i64(), 1, 50)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
        let body = req.json_body();
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
        let body = req.json_body();
        let import_id = require_str(&body, "importId")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
        let integration_name = require_str(&body, "integrationName")?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
        let body = req.json_body();
        let integration_name = require_str(&body, "integrationName")?;
        validate_string_length("integrationName", integration_name, 1, 50)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.integrations.remove(integration_name);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn list_integrations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
        let body = req.json_body();
        let lookup_table_name = require_str(&body, "lookupTableName")?;
        validate_string_length("lookupTableName", lookup_table_name, 1, 256)?;
        let table_body = require_str(&body, "tableBody")?;
        validate_string_length("tableBody", table_body, 1, 10485760)?;
        validate_optional_string_length("description", body["description"].as_str(), 0, 1024)?;
        validate_optional_string_length("kmsKeyId", body["kmsKeyId"].as_str(), 0, 256)?;

        let account_id = req.account_id.clone();
        let region = req.region.clone();

        let arn = Arn::new(
            "logs",
            &region,
            &account_id,
            &format!("lookup-table:{lookup_table_name}"),
        )
        .to_string();
        let now = Utc::now().timestamp_millis();

        let table = LookupTable {
            lookup_table_name: lookup_table_name.to_string(),
            arn: arn.clone(),
            table_body: table_body.to_string(),
            creation_time: now,
            last_modified_time: now,
            description: body["description"].as_str().map(str::to_string),
            kms_key_id: body["kmsKeyId"].as_str().map(str::to_string),
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
        let lookup_table_arn = require_str(&body, "lookupTableArn")?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        match state.lookup_tables.get(lookup_table_arn) {
            Some(t) => {
                let mut out = json!({
                    "lookupTableName": t.lookup_table_name,
                    "lookupTableArn": t.arn,
                    "tableBody": t.table_body,
                    "creationTime": t.creation_time,
                    "lastModifiedTime": t.last_modified_time,
                });
                if let Some(ref d) = t.description {
                    out["description"] = json!(d);
                }
                if let Some(ref k) = t.kms_key_id {
                    out["kmsKeyId"] = json!(k);
                }
                Ok(AwsResponse::json(
                    StatusCode::OK,
                    serde_json::to_string(&out).unwrap(),
                ))
            }
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
        let body = req.json_body();
        validate_optional_string_length(
            "lookupTableNamePrefix",
            body["lookupTableNamePrefix"].as_str(),
            1,
            256,
        )?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 100)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
        let body = req.json_body();
        let lookup_table_arn = require_str(&body, "lookupTableArn")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.lookup_tables.remove(lookup_table_arn);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn update_lookup_table(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let lookup_table_arn = require_str(&body, "lookupTableArn")?;
        let table_body = require_str(&body, "tableBody")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        match state.lookup_tables.get_mut(lookup_table_arn) {
            Some(t) => {
                t.table_body = table_body.to_string();
                if let Some(d) = body["description"].as_str() {
                    t.description = Some(d.to_string());
                }
                if let Some(k) = body["kmsKeyId"].as_str() {
                    t.kms_key_id = Some(k.to_string());
                }
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
        let body = req.json_body();
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

        let account_id = req.account_id.clone();
        let region = req.region.clone();

        let arn = Arn::new(
            "logs",
            &region,
            &account_id,
            &format!("scheduled-query:{name}"),
        )
        .to_string();
        let now = Utc::now().timestamp_millis();

        // `state` is the caller's desired ENABLED/DISABLED setting; live AWS
        // defaults it to ENABLED when omitted.
        let desired_state = body["state"].as_str().unwrap_or("ENABLED").to_string();

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
            description: body["description"].as_str().map(str::to_string),
            timezone: body["timezone"].as_str().map(str::to_string),
            schedule_start_time: body["scheduleStartTime"].as_i64(),
            schedule_end_time: body["scheduleEndTime"].as_i64(),
            state: Some(desired_state.clone()),
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.scheduled_queries.insert(arn.clone(), sq);

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "scheduledQueryArn": arn, "state": desired_state }))
                .unwrap(),
        ))
    }

    pub(crate) fn get_scheduled_query(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let identifier = require_str(&body, "identifier")?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        match state.scheduled_queries.get(identifier) {
            Some(sq) => {
                let mut out = json!({
                    "scheduledQueryArn": sq.arn,
                    "name": sq.name,
                    "queryString": sq.query_string,
                    "queryLanguage": sq.query_language,
                    "scheduleExpression": sq.schedule_expression,
                    "executionRoleArn": sq.execution_role_arn,
                    "creationTime": sq.creation_time,
                    "lastUpdatedTime": sq.last_modified_time,
                });
                if let Some(ref d) = sq.description {
                    out["description"] = json!(d);
                }
                if let Some(ref tz) = sq.timezone {
                    out["timezone"] = json!(tz);
                }
                if let Some(t) = sq.schedule_start_time {
                    out["scheduleStartTime"] = json!(t);
                }
                if let Some(t) = sq.schedule_end_time {
                    out["scheduleEndTime"] = json!(t);
                }
                if let Some(ref s) = sq.state {
                    out["state"] = json!(s);
                }
                Ok(AwsResponse::json(
                    StatusCode::OK,
                    serde_json::to_string(&out).unwrap(),
                ))
            }
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
        let body = req.json_body();
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
        let body = req.json_body();
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 1000)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;
        validate_optional_enum_value("state", &body["state"], &["ENABLED", "DISABLED"])?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
        let body = req.json_body();
        let identifier = require_str(&body, "identifier")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.scheduled_queries.remove(identifier);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn update_scheduled_query(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let identifier = require_str(&body, "identifier")?;
        let query_string = require_str(&body, "queryString")?;
        let query_language = require_str(&body, "queryLanguage")?;
        let schedule_expression = require_str(&body, "scheduleExpression")?;
        let execution_role_arn = require_str(&body, "executionRoleArn")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        match state.scheduled_queries.get_mut(identifier) {
            Some(sq) => {
                sq.query_string = query_string.to_string();
                sq.query_language = query_language.to_string();
                sq.schedule_expression = schedule_expression.to_string();
                sq.execution_role_arn = execution_role_arn.to_string();
                if let Some(d) = body["description"].as_str() {
                    sq.description = Some(d.to_string());
                }
                if let Some(tz) = body["timezone"].as_str() {
                    sq.timezone = Some(tz.to_string());
                }
                if let Some(t) = body["scheduleStartTime"].as_i64() {
                    sq.schedule_start_time = Some(t);
                }
                if let Some(t) = body["scheduleEndTime"].as_i64() {
                    sq.schedule_end_time = Some(t);
                }
                if let Some(st) = body["state"].as_str() {
                    sq.state = Some(st.to_string());
                }
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

    // -- Live tail / log object / log fields --

    pub(crate) fn start_live_tail(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("logGroupIdentifiers", &body["logGroupIdentifiers"])?;
        validate_optional_string_length(
            "logEventFilterPattern",
            body["logEventFilterPattern"].as_str(),
            0,
            1024,
        )?;
        let arr = body["logGroupIdentifiers"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "logGroupIdentifiers must be an array of strings",
            )
        })?;
        let mut identifiers: Vec<String> = Vec::with_capacity(arr.len());
        for v in arr {
            let s = v.as_str().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logGroupIdentifiers entries must be strings",
                )
            })?;
            identifiers.push(s.to_string());
        }
        let stream_filter = body["logStreamNames"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let pattern = body["logEventFilterPattern"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(String::from);

        // Real CWL StartLiveTail returns vnd.amazon.eventstream over HTTP/2:
        // each event-stream message carries exactly one `StartLiveTailResponseStream`
        // union variant (e.g. one `sessionStart`, then a sequence of
        // `sessionUpdate`s). fakecloud emulates the initial framed response by
        // returning the first variant (`sessionStart`) inside the JSON envelope.
        // Subsequent `sessionUpdate`s would be additional event-stream messages
        // in the real wire format. Returning both variants in one envelope
        // breaks the union contract (`responseStream` must hold exactly one
        // member), so SDKs that strictly decode the union would reject it.
        // Consume the unused state read + filter args so they don't show up
        // as dead-code warnings; live-tail history is exposed through
        // FilterLogEvents / GetLogEvents instead.
        let session_id = uuid::Uuid::new_v4().to_string();
        let _ = (&self.state, &stream_filter, &pattern);
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "responseStream": {
                    "sessionStart": {
                        "sessionId": session_id,
                        "requestId": req.request_id,
                        "logGroupIdentifiers": identifiers,
                        "logEventFilterPattern": pattern,
                        "logStreamNames": stream_filter,
                    },
                }
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn put_bearer_token_authentication(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required(
            "bearerTokenAuthenticationEnabled",
            &body["bearerTokenAuthenticationEnabled"],
        )?;
        let log_group_identifier = require_str(&body, "logGroupIdentifier")?;
        validate_string_length("logGroupIdentifier", log_group_identifier, 1, 2048)?;
        let enabled = body["bearerTokenAuthenticationEnabled"]
            .as_bool()
            .unwrap_or(false);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .bearer_token_auth
            .insert(log_group_identifier.to_string(), enabled);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(crate) fn get_log_object(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("logObjectPointer", &body["logObjectPointer"])?;
        validate_optional_string_length(
            "logObjectPointer",
            body["logObjectPointer"].as_str(),
            1,
            512,
        )?;
        // fakecloud-issued log object pointers are base64 of
        // `<group>|<stream>|<event_index>`. Real CWL pointers are opaque
        // tokens too, so callers cannot legitimately construct one
        // without first observing it; we only need to round-trip what
        // `Live tail` / `FilterLogEvents` produced.
        let pointer = body["logObjectPointer"].as_str().unwrap_or("");
        let (group_name, stream_name, idx) =
            parse_log_object_pointer(pointer).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "logObjectPointer is not a fakecloud-issued pointer",
                )
            })?;
        let accounts = self.state.read();
        let state = accounts.get(&req.account_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "log object not found",
            )
        })?;
        let group = state.log_groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("log group '{group_name}' not found"),
            )
        })?;
        let stream = group.log_streams.get(&stream_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("log stream '{stream_name}' not found"),
            )
        })?;
        let ev = stream.events.get(idx).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "log event index out of range",
            )
        })?;
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "fieldStream": {
                    "fields": {
                        "@timestamp": ev.timestamp.to_string(),
                        "@ingestionTime": ev.ingestion_time.to_string(),
                        "@logStream": stream_name,
                        "@logGroup": group_name,
                        "@message": ev.message,
                    }
                }
            }))
            .unwrap(),
        ))
    }

    pub(crate) fn get_log_fields(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("dataSourceName", &body["dataSourceName"])?;
        validate_required("dataSourceType", &body["dataSourceType"])?;
        let source_name = body["dataSourceName"].as_str().unwrap_or("");
        let source_type = body["dataSourceType"].as_str().unwrap_or("");
        if source_type != "LogGroup" {
            // Other source types (S3 source, integration) are not modeled.
            return Ok(AwsResponse::json(
                StatusCode::OK,
                serde_json::to_string(&json!({ "logFields": [] })).unwrap(),
            ));
        }
        let group_name = log_group_name_from_identifier(source_name);
        let accounts = self.state.read();
        let mut fields: std::collections::BTreeMap<String, &'static str> =
            std::collections::BTreeMap::new();
        // Always-present synthetic fields modeled by every log event.
        for (k, t) in [
            ("@timestamp", "long"),
            ("@ingestionTime", "long"),
            ("@logStream", "string"),
            ("@logGroup", "string"),
            ("@message", "string"),
        ] {
            fields.insert(k.to_string(), t);
        }
        if let Some(state) = accounts.get(&req.account_id) {
            if let Some(group) = state.log_groups.get(&group_name) {
                for stream in group.log_streams.values() {
                    for ev in &stream.events {
                        if let Ok(parsed) =
                            serde_json::from_str::<serde_json::Map<String, Value>>(&ev.message)
                        {
                            for (key, value) in parsed {
                                let kind = json_value_kind(&value);
                                fields.entry(key).or_insert(kind);
                            }
                        }
                    }
                }
            }
        }
        let log_fields: Vec<Value> = fields
            .into_iter()
            .map(|(name, ty)| json!({ "name": name, "type": ty }))
            .collect();
        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({ "logFields": log_fields })).unwrap(),
        ))
    }

    pub(crate) fn associate_source_to_s3_table_integration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("dataSource", &body["dataSource"])?;
        let integration_arn = require_str(&body, "integrationArn")?;
        let data_source = body["dataSource"].clone();
        let source_id = data_source
            .as_object()
            .and_then(|o| o.get("resourceArn"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let body = req.json_body();
        let integration_arn = require_str(&body, "integrationArn")?;
        validate_optional_range_i64("maxResults", body["maxResults"].as_i64(), 1, 100)?;
        validate_optional_string_length("nextToken", body["nextToken"].as_str(), 1, 4096)?;

        let accounts = self.state.read();
        let empty = crate::state::LogsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
        let body = req.json_body();
        let identifier = require_str(&body, "identifier")?.to_string();
        validate_string_length("identifier", &identifier, 1, 2048)?;

        // Remove the source identified by `identifier` from every integration
        // it was associated with, mirroring what AssociateSource recorded.
        // Previously a no-op, so ListSources kept returning a removed source.
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for sources in state.s3_table_sources.values_mut() {
            sources.retain(|s| s != &identifier);
        }
        state.s3_table_sources.retain(|_, v| !v.is_empty());
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }
}

/// Extract a log-group name from either a bare name or a CWL log-group ARN.
/// `arn:aws:logs:us-east-1:000000000000:log-group:foo:*` -> `foo`.
fn log_group_name_from_identifier(ident: &str) -> String {
    if let Some(rest) = ident.strip_prefix("arn:") {
        // arn:aws:logs:<region>:<account>:log-group:<name>[:*]
        let parts: Vec<&str> = rest.splitn(7, ':').collect();
        if parts.len() >= 7 && parts[4] == "log-group" {
            let tail = parts[5];
            return tail.split(':').next().unwrap_or(tail).to_string();
        }
    }
    ident.to_string()
}

/// Pointer format: base64(`<group>|<stream>|<index>`). Pointers are
/// only ever round-tripped through fakecloud — no external client
/// generates them — so the encoding is private but stable.
#[cfg(test)]
pub(crate) fn encode_log_object_pointer(group: &str, stream: &str, idx: usize) -> String {
    use base64::Engine;
    let raw = format!("{group}|{stream}|{idx}");
    base64::engine::general_purpose::STANDARD.encode(raw.as_bytes())
}

fn parse_log_object_pointer(pointer: &str) -> Option<(String, String, usize)> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(pointer)
        .ok()?;
    let raw = String::from_utf8(bytes).ok()?;
    let parts: Vec<&str> = raw.splitn(3, '|').collect();
    if parts.len() != 3 {
        return None;
    }
    let idx = parts[2].parse::<usize>().ok()?;
    Some((parts[0].to_string(), parts[1].to_string(), idx))
}

fn json_value_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "string",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "long",
        Value::Number(_) => "double",
        Value::String(_) => "string",
        Value::Array(_) => "list",
        Value::Object(_) => "map",
    }
}

#[cfg(test)]
mod tests {
    use crate::service::test_helpers::*;
    use serde_json::{json, Value};

    // ---- Misc operations ----

    #[test]
    fn get_log_group_fields_returns_synthetic_fields_when_no_events() {
        let svc = make_service();
        create_group(&svc, "fields-group");

        let req = make_request(
            "GetLogGroupFields",
            json!({ "logGroupName": "fields-group" }),
        );
        let resp = svc.get_log_group_fields(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let fields = body["logGroupFields"].as_array().unwrap();
        assert_eq!(fields.len(), 3);
        let names: Vec<&str> = fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"@timestamp"));
        assert!(names.contains(&"@message"));
        assert!(names.contains(&"@logStream"));
    }

    #[test]
    fn get_log_group_fields_extracts_top_level_keys_from_json_events() {
        let svc = make_service();
        create_group(&svc, "json-group");
        create_stream(&svc, "json-group", "s1");
        put_events(
            &svc,
            "json-group",
            "s1",
            &[
                r#"{"level":"INFO","msg":"hello","userId":"u1"}"#,
                r#"{"level":"ERROR","msg":"bad","trace":"…"}"#,
            ],
        );

        let req = make_request("GetLogGroupFields", json!({ "logGroupName": "json-group" }));
        let resp = svc.get_log_group_fields(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let fields = body["logGroupFields"].as_array().unwrap();
        let names: Vec<&str> = fields.iter().map(|f| f["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"level"));
        assert!(names.contains(&"msg"));
        assert!(names.contains(&"userId"));
        assert!(names.contains(&"trace"));
        // userId only in first event, trace only in second -> 50% each.
        let user_id = fields
            .iter()
            .find(|f| f["name"].as_str().unwrap() == "userId")
            .unwrap();
        assert_eq!(user_id["percent"].as_i64().unwrap(), 50);
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let qid = body["queryId"].as_str().unwrap().to_string();

        // Manually set query status to Running so we can test cancellation
        {
            let mut _mas = svc.state.write();
            let state = _mas.default_mut();
            state.queries.get_mut(&qid).unwrap().status = "Running".to_string();
        }

        let req = make_request("StopQuery", json!({ "queryId": &qid }));
        let resp = svc.stop_query(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["success"], true);

        let _mas = svc.state.read();
        let state = _mas.default_ref();
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

        let _mas = svc.state.read();
        let state = _mas.default_ref();
        assert!(state.log_groups["prot-group"].deletion_protection);
    }

    #[test]
    fn get_log_record_rejects_non_fakecloud_pointer() {
        // GetLogRecord pointers are now validated as fakecloud-issued
        // (base64 of `<group>|<stream>|<index>`). Non-conforming pointers
        // are rejected with InvalidParameterException, matching how real
        // CWL would reject a pointer it didn't issue.
        let svc = make_service();
        let req = make_request(
            "GetLogRecord",
            json!({ "logRecordPointer": "some-pointer" }),
        );
        match svc.get_log_record(&req) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("expected InvalidParameterException for non-fakecloud pointer"),
        }
    }

    #[test]
    fn list_anomalies_returns_empty() {
        let svc = make_service();

        let req = make_request("ListAnomalies", json!({}));
        let resp = svc.list_anomalies(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let import_id = body["importId"].as_str().unwrap().to_string();

        let req = make_request("DescribeImportTasks", json!({}));
        let resp = svc.describe_import_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["imports"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DescribeImportTaskBatches",
            json!({ "importId": import_id }),
        );
        let resp = svc.describe_import_task_batches(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["importBatches"].as_array().unwrap().is_empty());

        let req = make_request("CancelImportTask", json!({ "importId": import_id }));
        svc.cancel_import_task(&req).unwrap();

        let req = make_request("DescribeImportTasks", json!({}));
        let resp = svc.describe_import_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["integrationName"].as_str().unwrap(), "test-int");

        let req = make_request("ListIntegrations", json!({}));
        let resp = svc.list_integrations(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["integrationSummaries"].as_array().unwrap().len(), 1);

        let req = make_request(
            "DeleteIntegration",
            json!({ "integrationName": "test-int" }),
        );
        svc.delete_integration(&req).unwrap();

        let req = make_request("ListIntegrations", json!({}));
        let resp = svc.list_integrations(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["lookupTableArn"].as_str().unwrap().to_string();

        let req = make_request("GetLookupTable", json!({ "lookupTableArn": arn }));
        let resp = svc.get_lookup_table(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["lookupTableName"].as_str().unwrap(), "test-table");

        let req = make_request("DescribeLookupTables", json!({}));
        let resp = svc.describe_lookup_tables(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["scheduledQueryArn"].as_str().unwrap().to_string();

        let req = make_request("GetScheduledQuery", json!({ "identifier": arn }));
        let resp = svc.get_scheduled_query(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["name"].as_str().unwrap(), "test-sq");

        let req = make_request(
            "GetScheduledQueryHistory",
            json!({ "identifier": arn, "startTime": 0_i64, "endTime": 9999999999_i64 }),
        );
        let resp = svc.get_scheduled_query_history(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["triggerHistory"].as_array().unwrap().is_empty());

        let req = make_request("ListScheduledQueries", json!({}));
        let resp = svc.list_scheduled_queries(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["scheduledQueries"].as_array().unwrap().is_empty());
    }

    // CreateLookupTable persists description + kmsKeyId; GetLookupTable echoes
    // them (bug-hunt 2026-07-19).
    #[test]
    fn lookup_table_persists_description_and_kms_key() {
        let svc = make_service();
        let req = make_request(
            "CreateLookupTable",
            json!({
                "lookupTableName": "t",
                "tableBody": "key,value\na,b",
                "description": "my table",
                "kmsKeyId": "arn:aws:kms:us-east-1:123456789012:key/abc"
            }),
        );
        let resp = svc.create_lookup_table(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arn = body["lookupTableArn"].as_str().unwrap().to_string();

        let req = make_request("GetLookupTable", json!({ "lookupTableArn": arn.clone() }));
        let resp = svc.get_lookup_table(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["description"].as_str().unwrap(), "my table");
        assert_eq!(
            body["kmsKeyId"].as_str().unwrap(),
            "arn:aws:kms:us-east-1:123456789012:key/abc"
        );

        // UpdateLookupTable merges a new description.
        let req = make_request(
            "UpdateLookupTable",
            json!({ "lookupTableArn": arn.clone(), "tableBody": "key,value\nc,d", "description": "updated" }),
        );
        svc.update_lookup_table(&req).unwrap();
        let req = make_request("GetLookupTable", json!({ "lookupTableArn": arn }));
        let resp = svc.get_lookup_table(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["description"].as_str().unwrap(), "updated");
    }

    // CreateScheduledQuery persists description/timezone/schedule times/state;
    // GetScheduledQuery echoes them and an update can disable it (bug-hunt
    // 2026-07-19).
    #[test]
    fn scheduled_query_persists_optional_fields_and_state() {
        let svc = make_service();
        let req = make_request(
            "CreateScheduledQuery",
            json!({
                "name": "sq",
                "queryString": "fields @timestamp",
                "queryLanguage": "CWLI",
                "scheduleExpression": "rate(1 hour)",
                "executionRoleArn": "arn:aws:iam::123456789012:role/exec",
                "description": "nightly",
                "timezone": "UTC",
                "scheduleStartTime": 1000_i64,
                "scheduleEndTime": 2000_i64,
                "state": "ENABLED"
            }),
        );
        let resp = svc.create_scheduled_query(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["state"].as_str().unwrap(), "ENABLED");
        let arn = body["scheduledQueryArn"].as_str().unwrap().to_string();

        let req = make_request("GetScheduledQuery", json!({ "identifier": arn.clone() }));
        let resp = svc.get_scheduled_query(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["description"].as_str().unwrap(), "nightly");
        assert_eq!(body["timezone"].as_str().unwrap(), "UTC");
        assert_eq!(body["scheduleStartTime"].as_i64().unwrap(), 1000);
        assert_eq!(body["scheduleEndTime"].as_i64().unwrap(), 2000);
        assert_eq!(body["state"].as_str().unwrap(), "ENABLED");

        // An update can disable the query.
        let req = make_request(
            "UpdateScheduledQuery",
            json!({
                "identifier": arn.clone(),
                "queryString": "fields @message",
                "queryLanguage": "CWLI",
                "scheduleExpression": "rate(2 hours)",
                "executionRoleArn": "arn:aws:iam::123456789012:role/exec",
                "state": "DISABLED"
            }),
        );
        svc.update_scheduled_query(&req).unwrap();
        let req = make_request("GetScheduledQuery", json!({ "identifier": arn }));
        let resp = svc.get_scheduled_query(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["state"].as_str().unwrap(), "DISABLED");
    }

    // A scheduled query created without an explicit state defaults to ENABLED.
    #[test]
    fn scheduled_query_defaults_state_enabled() {
        let svc = make_service();
        let req = make_request(
            "CreateScheduledQuery",
            json!({
                "name": "sq2",
                "queryString": "fields @timestamp",
                "queryLanguage": "CWLI",
                "scheduleExpression": "rate(1 hour)",
                "executionRoleArn": "arn:aws:iam::123456789012:role/exec"
            }),
        );
        let resp = svc.create_scheduled_query(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["state"].as_str().unwrap(), "ENABLED");
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(body["responseStream"]["sessionStart"]["sessionId"]
            .as_str()
            .is_some());
        // `StartLiveTailResponseStream` is a Smithy union — the initial
        // framed response carries exactly one variant (sessionStart).
        let stream = body["responseStream"].as_object().unwrap();
        assert_eq!(stream.len(), 1);
        assert!(stream.contains_key("sessionStart"));
    }

    #[test]
    fn list_log_groups_delegates_to_describe() {
        let svc = make_service();
        create_group(&svc, "/test/list");
        let req = make_request("DescribeLogGroups", json!({}));
        let resp = svc.describe_log_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["logGroups"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn list_log_groups_for_query_unknown_query_errors() {
        let svc = make_service();
        let req = make_request(
            "ListLogGroupsForQuery",
            json!({ "queryId": "some-query-id" }),
        );
        match svc.list_log_groups_for_query(&req) {
            Err(e) => assert_eq!(e.code(), "ResourceNotFoundException"),
            Ok(_) => panic!("expected ResourceNotFoundException"),
        }
    }

    #[test]
    fn list_aggregate_log_group_summaries_returns_empty() {
        let svc = make_service();
        let req = make_request(
            "ListAggregateLogGroupSummaries",
            json!({ "groupBy": "DATA_SOURCE_NAME_AND_TYPE" }),
        );
        let resp = svc.list_aggregate_log_group_summaries(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    fn get_log_object_rejects_garbage_pointer() {
        let svc = make_service();
        let req = make_request("GetLogObject", json!({ "logObjectPointer": "not-base64" }));
        match svc.get_log_object(&req) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("expected InvalidParameterException"),
        }
    }

    #[test]
    fn get_log_object_returns_event_for_valid_pointer() {
        let svc = make_service();
        create_group(&svc, "/test/getobj");
        // Seed a stream + event.
        let req = make_request(
            "CreateLogStream",
            json!({ "logGroupName": "/test/getobj", "logStreamName": "s1" }),
        );
        svc.create_log_stream(&req).unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "/test/getobj",
                "logStreamName": "s1",
                "logEvents": [{ "timestamp": now, "message": "hello" }]
            }),
        );
        svc.put_log_events(&req).unwrap();

        let pointer = super::encode_log_object_pointer("/test/getobj", "s1", 0);
        let req = make_request("GetLogObject", json!({ "logObjectPointer": pointer }));
        let resp = svc.get_log_object(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["fieldStream"]["fields"]["@message"], "hello");
        assert_eq!(body["fieldStream"]["fields"]["@logStream"], "s1");
    }

    #[test]
    fn get_log_fields_extracts_keys_from_json_messages() {
        let svc = make_service();
        create_group(&svc, "/test/fields");
        let req = make_request(
            "CreateLogStream",
            json!({ "logGroupName": "/test/fields", "logStreamName": "s1" }),
        );
        svc.create_log_stream(&req).unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        let req = make_request(
            "PutLogEvents",
            json!({
                "logGroupName": "/test/fields",
                "logStreamName": "s1",
                "logEvents": [
                    { "timestamp": now, "message": "{\"level\":\"info\",\"latency_ms\":42}" },
                    { "timestamp": now, "message": "{\"level\":\"warn\",\"path\":\"/x\"}" }
                ]
            }),
        );
        svc.put_log_events(&req).unwrap();

        let req = make_request(
            "GetLogFields",
            json!({ "dataSourceName": "/test/fields", "dataSourceType": "LogGroup" }),
        );
        let resp = svc.get_log_fields(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let names: Vec<&str> = body["logFields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        for expected in ["@message", "@timestamp", "level", "latency_ms", "path"] {
            assert!(
                names.contains(&expected),
                "missing field {expected}: {names:?}"
            );
        }
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["sources"].as_array().unwrap().len(), 1);

        // Disassociating the source removes it; ListSources then returns none.
        let req = make_request(
            "DisassociateSourceFromS3TableIntegration",
            json!({ "identifier": "arn:aws:logs:us-east-1:123456789012:log-group:test" }),
        );
        svc.disassociate_source_from_s3_table_integration(&req)
            .unwrap();

        let req = make_request(
            "ListSourcesForS3TableIntegration",
            json!({
                "integrationArn": "arn:aws:logs:us-east-1:123456789012:integration:test"
            }),
        );
        let resp = svc.list_sources_for_s3_table_integration(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["sources"].as_array().unwrap().len(), 0);
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
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let delivery_id = body["delivery"]["id"].as_str().unwrap().to_string();

        let req = make_request("UpdateDeliveryConfiguration", json!({ "id": delivery_id }));
        svc.update_delivery_configuration(&req).unwrap();
    }

    #[test]
    fn describe_configuration_templates_returns_standard_set() {
        let svc = make_service();
        let req = make_request("DescribeConfigurationTemplates", json!({}));
        let resp = svc.describe_configuration_templates(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(!body["configurationTemplates"]
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

        {
            let _mas = svc.state.read();
            let state = _mas.default_ref();
            assert!(state.log_groups["dp-toggle"].deletion_protection);
        }

        // Disable
        let req = make_request(
            "PutLogGroupDeletionProtection",
            json!({
                "logGroupIdentifier": "dp-toggle",
                "deletionProtectionEnabled": false,
            }),
        );
        svc.put_log_group_deletion_protection(&req).unwrap();

        let _mas = svc.state.read();
        let state = _mas.default_ref();
        assert!(!state.log_groups["dp-toggle"].deletion_protection);
    }

    // ---- GetLogRecord ----

    #[test]
    fn get_log_record_rejects_arbitrary_pointer() {
        // Pointers must be fakecloud-issued — see streams.rs::get_log_record
        // for the encoding. This previously returned an empty stub; the
        // strict-validation behavior matches what real CWL does for an
        // un-minted pointer.
        let svc = make_service();
        let req = make_request(
            "GetLogRecord",
            json!({ "logRecordPointer": "any-pointer-value" }),
        );
        match svc.get_log_record(&req) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("expected InvalidParameterException for non-fakecloud pointer"),
        }
    }
}
