//! `AthenaService` `notebooks` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl AthenaService {
    pub(super) fn create_notebook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let name = require_str(&body, "Name")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.work_groups.contains_key(&work_group) {
            return Err(invalid_request(format!("Workgroup {work_group} not found")));
        }
        let id = synth_uuid();
        account.notebooks.insert(
            id.clone(),
            Notebook {
                notebook_id: id.clone(),
                name,
                work_group,
                creation_time: Utc::now(),
                last_modified_time: Utc::now(),
                payload: String::new(),
                notebook_type: "IPYNB".to_string(),
            },
        );
        Ok(AwsResponse::ok_json(json!({ "NotebookId": id })))
    }

    pub(super) fn import_notebook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let name = require_str(&body, "Name")?;
        let payload = body
            .get("Payload")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let notebook_type = body
            .get("Type")
            .and_then(Value::as_str)
            .unwrap_or("IPYNB")
            .to_string();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.work_groups.contains_key(&work_group) {
            return Err(invalid_request(format!("Workgroup {work_group} not found")));
        }
        let id = synth_uuid();
        account.notebooks.insert(
            id.clone(),
            Notebook {
                notebook_id: id.clone(),
                name,
                work_group,
                creation_time: Utc::now(),
                last_modified_time: Utc::now(),
                payload,
                notebook_type,
            },
        );
        Ok(AwsResponse::ok_json(json!({ "NotebookId": id })))
    }

    pub(super) fn export_notebook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NotebookId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let n = account
            .notebooks
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Notebook {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "NotebookMetadata": notebook_metadata_json(n),
            "Payload": n.payload,
        })))
    }

    pub(super) fn get_notebook_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NotebookId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let n = account
            .notebooks
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Notebook {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "NotebookMetadata": notebook_metadata_json(n),
        })))
    }

    pub(super) fn list_notebook_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let max_results = validate_max_results(&body, 1, 50)?;
        // Smithy: NextToken targets Token @length(1,1024).
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<Notebook> = account
            .notebooks
            .values()
            .filter(|n| n.work_group == work_group)
            .cloned()
            .collect();
        all.sort_by(|a, b| a.notebook_id.cmp(&b.notebook_id));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let metadatas: Vec<Value> = page.iter().map(notebook_metadata_json).collect();
        let mut response = json!({ "NotebookMetadataList": metadatas });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn update_notebook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NotebookId")?;
        let payload = require_str(&body, "Payload")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let n = account
            .notebooks
            .get_mut(&id)
            .ok_or_else(|| invalid_request(format!("Notebook {id} not found")))?;
        n.payload = payload;
        n.last_modified_time = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn update_notebook_metadata(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NotebookId")?;
        let name = require_str(&body, "Name")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let n = account
            .notebooks
            .get_mut(&id)
            .ok_or_else(|| invalid_request(format!("Notebook {id} not found")))?;
        n.name = name;
        n.last_modified_time = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_notebook(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "NotebookId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.notebooks.remove(&id).is_none() {
            return Err(invalid_request(format!("Notebook {id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn create_presigned_notebook_url(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let session_id = require_str(&body, "SessionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.sessions.contains_key(&session_id) {
            return Err(invalid_request(format!("Session {session_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "NotebookUrl": format!("https://athena-notebook.{}.amazonaws.com/{}", req.region, session_id),
            "AuthToken": synth_uuid(),
            "AuthTokenExpirationTime": (Utc::now().timestamp() + 3600) as f64,
        })))
    }

    pub(super) fn start_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let notebook_id = body
            .get("NotebookId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let configuration = body.get("EngineConfiguration").cloned();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.work_groups.contains_key(&work_group) {
            return Err(invalid_request(format!("Workgroup {work_group} not found")));
        }
        let id = synth_uuid();
        account.sessions.insert(
            id.clone(),
            Session {
                session_id: id.clone(),
                work_group,
                notebook_id,
                description,
                engine_version: Some("PySpark engine version 3".to_string()),
                state: "IDLE".to_string(),
                start_date_time: Utc::now(),
                end_date_time: None,
                idle_since_date_time: Some(Utc::now()),
                configuration,
                notebook_version: Some("Athena notebook version 1".to_string()),
            },
        );
        Ok(AwsResponse::ok_json(json!({
            "SessionId": id,
            "State": "IDLE",
        })))
    }

    pub(super) fn get_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "SessionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let s = account
            .sessions
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Session {id} not found")))?;
        Ok(AwsResponse::ok_json(session_detail_json(s)))
    }

    pub(super) fn get_session_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "SessionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let s = account
            .sessions
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Session {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "SessionId": s.session_id,
            "Status": session_status_json(s),
        })))
    }

    pub(super) fn get_session_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "SessionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.sessions.contains_key(&id) {
            return Err(invalid_request(format!("Session {id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "SessionId": id,
            "EndpointUrl": format!("https://athena-session.{}.amazonaws.com/{}", req.region, id),
            "ExpirationDateTime": (Utc::now().timestamp() + 3600) as f64,
        })))
    }

    pub(super) fn list_sessions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = require_str(&body, "WorkGroup")?;
        // Smithy: MaxResults targets MaxSessionsCount @range(1,100);
        // NextToken targets SessionManagerToken @length(0,2048);
        // StateFilter is SessionState enum.
        let max_results = validate_max_results(&body, 1, 100)?;
        validate_opt_string_len(&body, "NextToken", 0, 2048)?;
        validate_opt_enum(
            &body,
            "StateFilter",
            &[
                "CREATING",
                "CREATED",
                "IDLE",
                "BUSY",
                "TERMINATING",
                "TERMINATED",
                "DEGRADED",
                "FAILED",
            ],
        )?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let state_filter = body
            .get("StateFilter")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<Session> = account
            .sessions
            .values()
            .filter(|s| s.work_group == work_group)
            .filter(|s| {
                state_filter
                    .as_deref()
                    .map(|sf| s.state == sf)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        all.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let summaries: Vec<Value> = page.iter().map(session_summary_json).collect();
        let mut response = json!({ "Sessions": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn list_notebook_sessions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let notebook_id = require_str(&body, "NotebookId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.notebooks.contains_key(&notebook_id) {
            return Err(invalid_request(format!("Notebook {notebook_id} not found")));
        }
        let summaries: Vec<Value> = account
            .sessions
            .values()
            .filter(|s| s.notebook_id.as_deref() == Some(notebook_id.as_str()))
            .map(session_summary_json)
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "NotebookSessionsList": summaries,
        })))
    }

    pub(super) fn terminate_session(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "SessionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let s = account
            .sessions
            .get_mut(&id)
            .ok_or_else(|| invalid_request(format!("Session {id} not found")))?;
        s.state = "TERMINATED".to_string();
        s.end_date_time = Some(Utc::now());
        Ok(AwsResponse::ok_json(json!({ "State": "TERMINATED" })))
    }

    pub(super) fn start_calculation_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let session_id = require_str(&body, "SessionId")?;
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let code_block = body
            .get("CodeBlock")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.sessions.contains_key(&session_id) {
            return Err(invalid_request(format!("Session {session_id} not found")));
        }
        let id = synth_uuid();
        account.calculations.insert(
            id.clone(),
            Calculation {
                calculation_execution_id: id.clone(),
                session_id,
                description,
                state: "COMPLETED".to_string(),
                state_change_reason: None,
                working_directory: Some(format!("s3://athena-calc-results/{}", Uuid::new_v4())),
                code_block,
                submission_date_time: Utc::now(),
                completion_date_time: Some(Utc::now()),
            },
        );
        Ok(AwsResponse::ok_json(json!({
            "CalculationExecutionId": id,
            "State": "COMPLETED",
        })))
    }

    pub(super) fn stop_calculation_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "CalculationExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let c = account
            .calculations
            .get_mut(&id)
            .ok_or_else(|| invalid_request(format!("Calculation {id} not found")))?;
        c.state = "CANCELED".to_string();
        c.state_change_reason = Some("Cancelled by user".to_string());
        Ok(AwsResponse::ok_json(json!({ "State": "CANCELED" })))
    }

    pub(super) fn get_calculation_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "CalculationExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let c = account
            .calculations
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Calculation {id} not found")))?;
        Ok(AwsResponse::ok_json(calculation_detail_json(c)))
    }

    pub(super) fn get_calculation_execution_code(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "CalculationExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let c = account
            .calculations
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Calculation {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "CodeBlock": c.code_block.clone().unwrap_or_default(),
        })))
    }

    pub(super) fn get_calculation_execution_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "CalculationExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let c = account
            .calculations
            .get(&id)
            .ok_or_else(|| invalid_request(format!("Calculation {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "Status": calculation_status_json(c),
            "Statistics": {
                "DpuExecutionInMillis": 100,
                "Progress": "100%",
            }
        })))
    }

    pub(super) fn list_calculation_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: SessionId @length(1,256), NextToken @length(0,2048),
        // StateFilter is CalculationExecutionState enum, MaxResults @range(1,100).
        let session_id = validate_required_string_len(&body, "SessionId", 1, 256)?;
        validate_opt_string_len(&body, "NextToken", 0, 2048)?;
        validate_opt_enum(
            &body,
            "StateFilter",
            &[
                "CREATING",
                "CREATED",
                "QUEUED",
                "RUNNING",
                "CANCELING",
                "CANCELED",
                "COMPLETED",
                "FAILED",
            ],
        )?;
        let max_results = validate_max_results(&body, 1, 100)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let state_filter = body
            .get("StateFilter")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<Calculation> = account
            .calculations
            .values()
            .filter(|c| c.session_id == session_id)
            .filter(|c| {
                state_filter
                    .as_deref()
                    .map(|sf| c.state == sf)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        all.sort_by(|a, b| a.calculation_execution_id.cmp(&b.calculation_execution_id));
        let (page, next) = paginate(&all, next_token.as_deref(), max_results);
        let summaries: Vec<Value> = page.iter().map(calculation_summary_json).collect();
        let mut response = json!({ "Calculations": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }
}
