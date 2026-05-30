//! `StepFunctionsService` `executions` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl StepFunctionsService {
    // ─── Execution Lifecycle ──────────────────────────────────────────

    pub(super) fn start_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("stateMachineArn", &body["stateMachineArn"])?;
        let sm_arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?;
        validate_arn(sm_arn)?;

        let input = body["input"].as_str().map(|s| s.to_string());

        // Validate input is valid JSON if provided
        if let Some(ref input_str) = input {
            let _: serde_json::Value = serde_json::from_str(input_str).map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidExecutionInput",
                    "Invalid execution input: must be valid JSON".to_string(),
                )
            })?;
        }

        let execution_name = body["name"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        if let Some(name) = body["name"].as_str() {
            validate_name(name)?;
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let sm = state
            .state_machines
            .get(sm_arn)
            .ok_or_else(|| state_machine_not_found(sm_arn))?;

        let sm_name = sm.name.clone();
        let definition = sm.definition.clone();
        let exec_arn = state.execution_arn(&sm_name, &execution_name);

        // Check for duplicate execution name
        if state.executions.contains_key(&exec_arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ExecutionAlreadyExists",
                format!("Execution Already Exists: '{exec_arn}'"),
            ));
        }

        let now = Utc::now();
        let execution = Execution {
            execution_arn: exec_arn.clone(),
            state_machine_arn: sm_arn.to_string(),
            state_machine_name: sm_name,
            name: execution_name,
            status: ExecutionStatus::Running,
            input: input.clone(),
            output: None,
            start_date: now,
            stop_date: None,
            error: None,
            cause: None,
            history_events: vec![],
            parent_execution_arn: None,
            is_sync: false,
            billed_duration_ms: None,
            billed_memory_mb: None,
        };

        state.executions.insert(exec_arn.clone(), execution);
        let logging_config = sm.logging_configuration.clone();
        drop(accounts);

        // Spawn async execution
        let shared_state = self.state.clone();
        let exec_arn_clone = exec_arn.clone();
        let input_clone = input;
        let delivery = self.delivery.clone();
        let dynamodb_state = self.dynamodb_state.clone();
        let registry = self.registry.clone();
        tokio::spawn(async move {
            interpreter::execute_state_machine(
                shared_state,
                exec_arn_clone,
                definition,
                input_clone,
                delivery,
                dynamodb_state,
                registry,
                logging_config,
            )
            .await;
        });

        Ok(AwsResponse::ok_json(json!({
            "executionArn": exec_arn,
            "startDate": now.timestamp() as f64,
        })))
    }

    pub(super) fn stop_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("executionArn", &body["executionArn"])?;
        let exec_arn = body["executionArn"]
            .as_str()
            .ok_or_else(|| missing("executionArn"))?;

        let error = body["error"].as_str().map(|s| s.to_string());
        let cause = body["cause"].as_str().map(|s| s.to_string());

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let exec = state
            .executions
            .get_mut(exec_arn)
            .ok_or_else(|| execution_not_found(exec_arn))?;

        if exec.status != ExecutionStatus::Running {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ExecutionNotRunning",
                format!("Execution is not running: '{exec_arn}'"),
            ));
        }

        let now = Utc::now();
        exec.status = ExecutionStatus::Aborted;
        exec.stop_date = Some(now);
        exec.error = error;
        exec.cause = cause;

        Ok(AwsResponse::ok_json(json!({
            "stopDate": now.timestamp() as f64,
        })))
    }

    pub(super) fn describe_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("executionArn", &body["executionArn"])?;
        let exec_arn = body["executionArn"]
            .as_str()
            .ok_or_else(|| missing("executionArn"))?;

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let exec = state
            .executions
            .get(exec_arn)
            .ok_or_else(|| execution_not_found(exec_arn))?;

        Ok(AwsResponse::ok_json(execution_to_json(exec)))
    }

    pub(super) fn list_executions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("stateMachineArn", &body["stateMachineArn"])?;
        let sm_arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?;
        validate_arn(sm_arn)?;

        let max_results = body["maxResults"].as_i64().unwrap_or(100) as usize;
        validate_range_i64("maxResults", max_results as i64, 1, 1000)?;
        let next_token = body["nextToken"].as_str();
        let status_filter = body["statusFilter"].as_str();

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        // Verify state machine exists
        if !state.state_machines.contains_key(sm_arn) {
            return Err(state_machine_not_found(sm_arn));
        }

        let mut executions: Vec<&Execution> = state
            .executions
            .values()
            .filter(|e| e.state_machine_arn == sm_arn)
            .filter(|e| {
                status_filter
                    .map(|sf| e.status.as_str() == sf)
                    .unwrap_or(true)
            })
            .collect();

        // Sort by start date descending (most recent first)
        executions.sort_by_key(|e| std::cmp::Reverse(e.start_date));

        let items: Vec<Value> = executions
            .iter()
            .map(|e| {
                let mut item = json!({
                    "executionArn": e.execution_arn,
                    "stateMachineArn": e.state_machine_arn,
                    "name": e.name,
                    "status": e.status.as_str(),
                    "startDate": e.start_date.timestamp() as f64,
                });
                if let Some(stop) = e.stop_date {
                    item["stopDate"] = json!(stop.timestamp() as f64);
                }
                item
            })
            .collect();

        let (page, token) = paginate(&items, next_token, max_results);

        let mut resp = json!({ "executions": page });
        if let Some(t) = token {
            resp["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn get_execution_history(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("executionArn", &body["executionArn"])?;
        let exec_arn = body["executionArn"]
            .as_str()
            .ok_or_else(|| missing("executionArn"))?;
        validate_arn_length("executionArn", exec_arn, 256)?;

        if let Some(mr) = body["maxResults"].as_i64() {
            validate_max_results(mr)?;
        }
        let max_results = body["maxResults"].as_i64().unwrap_or(100) as usize;
        let next_token = body["nextToken"].as_str();
        if let Some(t) = next_token {
            validate_page_token(t)?;
        }
        let reverse_order = body["reverseOrder"].as_bool().unwrap_or(false);

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let exec = state
            .executions
            .get(exec_arn)
            .ok_or_else(|| execution_not_found(exec_arn))?;

        let mut events: Vec<Value> = exec
            .history_events
            .iter()
            .map(|e| {
                json!({
                    "id": e.id,
                    "type": e.event_type,
                    "timestamp": e.timestamp.timestamp() as f64,
                    "previousEventId": e.previous_event_id,
                    format!("{}EventDetails", camel_to_details_key(&e.event_type)): e.details,
                })
            })
            .collect();

        if reverse_order {
            events.reverse();
        }

        let (page, token) = paginate(&events, next_token, max_results);

        let mut resp = json!({ "events": page });
        if let Some(t) = token {
            resp["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    // ─── Execution lifecycle extras ─────────────────────────────────────

    pub(super) fn redrive_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = body["executionArn"]
            .as_str()
            .ok_or_else(|| missing("executionArn"))?
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let exec = state.executions.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ExecutionDoesNotExist",
                format!("Execution does not exist: {arn}"),
            )
        })?;
        exec.status = crate::state::ExecutionStatus::Running;
        exec.stop_date = None;
        Ok(AwsResponse::ok_json(json!({
            "redriveDate": chrono::Utc::now().timestamp(),
        })))
    }

    pub(super) async fn start_sync_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let sm_arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?
            .to_string();
        let input = body["input"].as_str().unwrap_or("{}").to_string();
        if serde_json::from_str::<serde_json::Value>(&input).is_err() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidExecutionInput",
                "Execution input is not valid JSON.",
            ));
        }
        let (exec_arn, definition, logging_config) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            let sm = state
                .state_machines
                .get(&sm_arn)
                .ok_or_else(|| state_machine_not_found(&sm_arn))?;
            if sm.machine_type != crate::state::StateMachineType::Express {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "StateMachineTypeNotSupported",
                    "StartSyncExecution is only supported for EXPRESS state machines.",
                ));
            }
            let now = chrono::Utc::now();
            // Mint a unique execution name with a UUID rather than a
            // millisecond timestamp: Express workflows start concurrently, so a
            // ms-resolution name collides and same-ms starts overwrote each
            // other (bug-audit 2026-05-28, 4.2). Loop on the (vanishingly
            // unlikely) UUID collision, mirroring the async StartExecution path.
            let (exec_name, exec_arn) = loop {
                let candidate = format!("sync-{}", uuid::Uuid::new_v4());
                let arn = format!(
                    "arn:aws:states:{}:{}:express:{}:{}",
                    state.region, state.account_id, sm.name, candidate
                );
                if !state.executions.contains_key(&arn) {
                    break (candidate, arn);
                }
            };
            let execution = Execution {
                execution_arn: exec_arn.clone(),
                state_machine_arn: sm_arn.clone(),
                state_machine_name: sm.name.clone(),
                name: exec_name.clone(),
                status: ExecutionStatus::Running,
                input: Some(input.clone()),
                output: None,
                start_date: now,
                stop_date: None,
                error: None,
                cause: None,
                history_events: vec![],
                parent_execution_arn: None,
                is_sync: true,
                billed_duration_ms: None,
                billed_memory_mb: None,
            };
            state.executions.insert(exec_arn.clone(), execution);
            (
                exec_arn,
                sm.definition.clone(),
                sm.logging_configuration.clone(),
            )
        };

        interpreter::execute_state_machine(
            self.state.clone(),
            exec_arn.clone(),
            definition,
            Some(input),
            self.delivery.clone(),
            self.dynamodb_state.clone(),
            self.registry.clone(),
            logging_config,
        )
        .await;

        // Persist billing details on the stored execution so introspection
        // endpoints can replay the same numbers later.
        {
            let mut accounts = self.state.write();
            if let Some(state) = accounts.get_mut(&req.account_id) {
                if let Some(exec) = state.executions.get_mut(&exec_arn) {
                    let duration_ms = exec
                        .stop_date
                        .map_or(0, |stop| (stop - exec.start_date).num_milliseconds())
                        .max(0);
                    exec.billed_duration_ms = Some(duration_ms);
                    exec.billed_memory_mb = Some(64);
                }
            }
        }

        let accounts = self.state.read();
        let state = accounts.get(&req.account_id).unwrap();
        let exec = state
            .executions
            .get(&exec_arn)
            .ok_or_else(|| execution_not_found(&exec_arn))?;

        let mut resp = json!({
            "executionArn": exec.execution_arn,
            "stateMachineArn": exec.state_machine_arn,
            "name": exec.name,
            "startDate": exec.start_date.timestamp(),
            "stopDate": exec.stop_date.map(|d| d.timestamp()),
            "status": exec.status.as_str(),
            "input": exec.input.as_deref().unwrap_or("{}"),
        });

        if let Some(ref output) = exec.output {
            resp["output"] = json!(output);
        }
        if let Some(ref error) = exec.error {
            resp["error"] = json!(error);
        }
        if let Some(ref cause) = exec.cause {
            resp["cause"] = json!(cause);
        }

        let duration_ms = exec
            .stop_date
            .map_or(0, |stop| (stop - exec.start_date).num_milliseconds());
        resp["billingDetails"] = json!({
            "billedMemoryUsedInMB": 64,
            "billedDurationInMilliseconds": duration_ms.max(0),
        });

        Ok(AwsResponse::ok_json(resp))
    }
}
