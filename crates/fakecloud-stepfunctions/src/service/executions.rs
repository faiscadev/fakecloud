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

        // AWS records an omitted input as `{}` (DescribeExecution, history and
        // a `.sync` parent's view of its child all show it).
        let input_text = body["input"].as_str().unwrap_or("{}");
        serde_json::from_str::<serde_json::Value>(input_text).map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidExecutionInput",
                "Invalid execution input: must be valid JSON".to_string(),
            )
        })?;
        let input = Some(input_text.to_string());

        let execution_name = body["name"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        if let Some(name) = body["name"].as_str() {
            validate_name(name)?;
        }

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let sm = state
            .state_machines
            .get(sm_arn)
            .ok_or_else(|| state_machine_not_found(sm_arn))?;

        let sm_name = sm.name.clone();
        let sm_role_arn = sm.role_arn.clone();
        let sm_type = sm.machine_type;
        let definition = sm.definition.clone();
        // EXPRESS names can be reused immediately and StartExecution is never
        // idempotent for them: every call gets its own ARN, so only STANDARD
        // has collisions to handle.
        let exec_arn = state.new_execution_arn(&req.region, sm_type, &sm_name, &execution_name);

        // STANDARD StartExecution is idempotent while the execution runs:
        // re-issuing the same name AND the same input returns the existing
        // executionArn with HTTP 200, while a different input or a closed
        // execution is a 400 ExecutionAlreadyExists.
        let existing = match sm_type {
            crate::state::StateMachineType::Standard => state.executions.get(&exec_arn),
            crate::state::StateMachineType::Express => None,
        };
        if let Some(existing) = existing {
            let same_input = existing.status == ExecutionStatus::Running
                && execution_input_matches(existing.input.as_deref(), input.as_deref());
            if same_input {
                return Ok(AwsResponse::ok_json(json!({
                    "executionArn": existing.execution_arn,
                    "startDate": existing.start_date.timestamp_millis() as f64 / 1000.0,
                })));
            }
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
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
            parent_execution_arn: crate::state::STARTED_BY_EXECUTION
                .try_with(Clone::clone)
                .ok(),
            is_sync: false,
            billed_duration_ms: None,
            billed_memory_mb: None,
            role_arn: sm_role_arn,
            redrive_count: 0,
            map_run_arn: None,
            item_count: None,
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
        // The interpreter flips the execution to its terminal status
        // (Succeeded/Failed/TimedOut) directly in shared state, outside the
        // action-dispatch path that snapshots -- so the post-StartExecution
        // save below only ever captured the RUNNING execution. Without writing
        // through when the interpreter finishes, a completed execution reported
        // stale RUNNING (with no output/history) after a restart, and an
        // in-flight one came back RUNNING forever with no driver to advance it
        // (bug-audit 2026-06-20, 0.A2).
        let snapshot_store = self.snapshot_store.clone();
        let snapshot_lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            interpreter::execute_state_machine(
                shared_state.clone(),
                exec_arn_clone,
                definition,
                input_clone,
                delivery,
                dynamodb_state,
                registry,
                logging_config,
            )
            .await;
            super::save_stepfunctions_snapshot(&shared_state, snapshot_store, &snapshot_lock).await;
        });

        Ok(AwsResponse::ok_json(json!({
            "executionArn": exec_arn,
            "startDate": now.timestamp_millis() as f64 / 1000.0,
        })))
    }

    pub(super) fn stop_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("executionArn", &body["executionArn"])?;
        let exec_arn = body["executionArn"]
            .as_str()
            .ok_or_else(|| missing("executionArn"))?;

        reject_express_execution_arn(exec_arn)?;
        let error = body["error"].as_str().map(|s| s.to_string());
        let cause = body["cause"].as_str().map(|s| s.to_string());

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let exec = state
            .executions
            .get_mut(exec_arn)
            .ok_or_else(|| execution_not_found(exec_arn))?;

        // As recorded, stopping an execution that has already ended changes
        // nothing and reports when it stopped.
        if let (true, Some(stopped)) = (exec.status != ExecutionStatus::Running, exec.stop_date) {
            return Ok(AwsResponse::ok_json(json!({
                "stopDate": stopped.timestamp_millis() as f64 / 1000.0,
            })));
        }

        let now = Utc::now();
        exec.status = ExecutionStatus::Aborted;
        exec.stop_date = Some(now);
        exec.error = error;
        exec.cause = cause;
        abort_map_runs(state, exec_arn, now);

        Ok(AwsResponse::ok_json(json!({
            "stopDate": now.timestamp_millis() as f64 / 1000.0,
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
        reject_unlabeled_express_execution_arn(exec_arn)?;

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let exec = state
            .executions
            .get(exec_arn)
            .ok_or_else(|| execution_not_found(exec_arn))?;

        Ok(AwsResponse::ok_json(execution_to_json(exec)))
    }

    pub(super) fn list_executions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Exactly one of the two, as recorded.
        let (sm_arn, map_run_arn) =
            match (body["stateMachineArn"].as_str(), body["mapRunArn"].as_str()) {
                (Some(sm_arn), None) => (Some(sm_arn), None),
                (None, Some(map_run_arn)) => (None, Some(map_run_arn)),
                _ => {
                    return Err(validation_exception(
                        "Must provide a StateMachine ARN or MapRun ARN",
                    ))
                }
            };

        let max_results = body["maxResults"].as_i64().unwrap_or(100) as usize;
        validate_range_i64("maxResults", max_results as i64, 1, 1000)?;
        let next_token = body["nextToken"].as_str();
        let status_filter = body["statusFilter"].as_str();

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);

        let in_scope: Box<dyn Fn(&Execution) -> bool> = match (sm_arn, map_run_arn) {
            (Some(sm_arn), _) => {
                validate_arn(sm_arn)?;
                if crate::state::is_labeled_arn(sm_arn) {
                    return Err(validation_exception(
                        "Labeled State Machine ARNs are not supported for this API",
                    ));
                }
                let sm = state
                    .state_machines
                    .get(sm_arn)
                    .ok_or_else(|| state_machine_not_found(sm_arn))?;
                if sm.machine_type == crate::state::StateMachineType::Express {
                    return Err(state_machine_type_not_supported(
                        "This operation is not supported by this type of state machine",
                    ));
                }
                Box::new(move |e: &Execution| e.state_machine_arn == sm_arn)
            }
            (None, Some(map_run_arn)) => {
                validate_arn(map_run_arn)?;
                let resource_type = map_run_arn.split(':').nth(5).unwrap_or_default();
                if resource_type != "mapRun" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidArn",
                        format!(
                            "Invalid Arn: 'Resource type not valid in this context: {resource_type}'"
                        ),
                    ));
                }
                if !state.map_runs.contains_key(map_run_arn) {
                    return Err(resource_not_found(map_run_arn));
                }
                // A map run's children, EXPRESS ones too, as recorded.
                Box::new(move |e: &Execution| e.map_run_arn.as_deref() == Some(map_run_arn))
            }
            (None, None) => unreachable!("one of the two is required above"),
        };

        let mut executions: Vec<&Execution> = state
            .executions
            .values()
            .filter(|e| in_scope(e))
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
                    "startDate": e.start_date.timestamp_millis() as f64 / 1000.0,
                    "redriveCount": e.redrive_count,
                });
                if let Some(stop) = e.stop_date {
                    item["stopDate"] = json!(stop.timestamp_millis() as f64 / 1000.0);
                }
                if let Some(map_run_arn) = &e.map_run_arn {
                    item["mapRunArn"] = json!(map_run_arn);
                    item["itemCount"] = json!(e.item_count.unwrap_or(1));
                }
                item
            })
            .collect();

        let (page, token) =
            paginate_checked(&items, next_token, max_results).map_err(|_| invalid_token())?;

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
        reject_express_execution_arn(exec_arn)?;

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
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let exec = state
            .executions
            .get(exec_arn)
            .ok_or_else(|| execution_not_found(exec_arn))?;

        let mut events: Vec<Value> = exec
            .history_events
            .iter()
            .map(|e| {
                let mut event = json!({
                    "id": e.id,
                    "type": e.event_type,
                    "timestamp": e.timestamp.timestamp_millis() as f64 / 1000.0,
                    "previousEventId": e.previous_event_id,
                });
                // Events with nothing to say (`MapRunSucceeded`,
                // `MapStateSucceeded`) carry no details, as recorded.
                if e.details.as_object().is_none_or(|d| !d.is_empty()) {
                    event[format!("{}EventDetails", camel_to_details_key(&e.event_type))] =
                        e.details.clone();
                }
                event
            })
            .collect();

        if reverse_order {
            events.reverse();
        }

        let (page, token) =
            paginate_checked(&events, next_token, max_results).map_err(|_| invalid_token())?;

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
        validate_required("executionArn", &body["executionArn"])?;
        let arn = body["executionArn"]
            .as_str()
            .ok_or_else(|| missing("executionArn"))?
            .to_string();
        reject_express_execution_arn(&arn)?;
        // A map run's children are redriven with their map run, as recorded.
        if crate::state::is_labeled_arn(&arn) {
            return Err(validation_exception(
                "Labeled Execution ARNs are not supported for this API",
            ));
        }

        // Reset the execution to Running and re-spawn the interpreter so a
        // redriven execution actually runs to completion. Without spawning a
        // driver the execution would hang RUNNING forever (bug-audit 4.6).
        let (definition, input, logging_config) = {
            let mut accounts = self.state.write();
            let state = accounts.regional_mut(&req.account_id, &req.region);
            let exec = state.executions.get(&arn).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ExecutionDoesNotExist",
                    format!("Execution does not exist: {arn}"),
                )
            })?;

            // Only terminal executions can be redriven; a RUNNING one already has
            // a driver, and redriving it would spawn a duplicate interpreter.
            if exec.status == crate::state::ExecutionStatus::Running {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ExecutionNotRedrivable",
                    format!("Execution is not redrivable: {arn}"),
                ));
            }

            let sm_arn = exec.state_machine_arn.clone();
            let input = exec.input.clone();
            let sm = state.state_machines.get(&sm_arn).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ExecutionNotRedrivable",
                    format!("State machine does not exist for execution: {arn}"),
                )
            })?;
            let definition = sm.definition.clone();
            let logging_config = sm.logging_configuration.clone();

            // Reset execution to a clean RUNNING state before re-running.
            let exec = state
                .executions
                .get_mut(&arn)
                .expect("execution presence checked above");
            exec.status = crate::state::ExecutionStatus::Running;
            exec.stop_date = None;
            exec.output = None;
            exec.error = None;
            exec.cause = None;
            exec.history_events.clear();
            exec.redrive_count = exec.redrive_count.saturating_add(1);

            (definition, input, logging_config)
        };

        // Spawn async execution (mirrors StartExecution).
        let shared_state = self.state.clone();
        let exec_arn_clone = arn.clone();
        let delivery = self.delivery.clone();
        let dynamodb_state = self.dynamodb_state.clone();
        let registry = self.registry.clone();
        let snapshot_store = self.snapshot_store.clone();
        let snapshot_lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            interpreter::execute_state_machine(
                shared_state.clone(),
                exec_arn_clone,
                definition,
                input,
                delivery,
                dynamodb_state,
                registry,
                logging_config,
            )
            .await;
            super::save_stepfunctions_snapshot(&shared_state, snapshot_store, &snapshot_lock).await;
        });

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
        validate_arn(&sm_arn)?;
        let exec_name = body["name"].as_str().map(str::to_string);
        if let Some(name) = &exec_name {
            validate_name(name)?;
        }
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
            let state = accounts.regional_mut(&req.account_id, &req.region);
            let sm = state
                .state_machines
                .get(&sm_arn)
                .ok_or_else(|| state_machine_not_found(&sm_arn))?;
            if sm.machine_type != crate::state::StateMachineType::Express {
                return Err(state_machine_type_not_supported(
                    "StartSyncExecution is only supported for EXPRESS state machines.",
                ));
            }
            let now = chrono::Utc::now();
            // The caller's name, else a UUID as AWS generates. The ARN carries
            // its own id, so concurrent starts (and reused names) never
            // overwrite each other (bug-audit 2026-05-28, 4.2).
            let exec_name = exec_name.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let exec_arn = state.new_execution_arn(
                &req.region,
                crate::state::StateMachineType::Express,
                &sm.name,
                &exec_name,
            );
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
                role_arn: sm.role_arn.clone(),
                redrive_count: 0,
                map_run_arn: None,
                item_count: None,
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
            if let Some(state) = accounts.regional_get_mut(&req.account_id, &req.region) {
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
        let state = accounts.regional(&req.account_id, &req.region).unwrap();
        let exec = state
            .executions
            .get(&exec_arn)
            .ok_or_else(|| execution_not_found(&exec_arn))?;

        let mut resp = json!({
            "executionArn": exec.execution_arn,
            "stateMachineArn": exec.state_machine_arn,
            "name": exec.name,
            "startDate": exec.start_date.timestamp_millis() as f64 / 1000.0,
            "stopDate": exec.stop_date.map(|d| d.timestamp_millis() as f64 / 1000.0),
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

/// A stopped execution's map runs stop with it, and so do their children
/// still running: the iterations that finish later leave them as they are.
fn abort_map_runs(state: &mut StepFunctionsState, execution_arn: &str, now: chrono::DateTime<Utc>) {
    let mut aborted = Vec::new();
    for run in state.map_runs.values_mut() {
        if run.execution_arn == execution_arn && run.status == "RUNNING" {
            run.status = "ABORTED".to_string();
            run.stop_date = Some(now);
            aborted.push(run.map_run_arn.clone());
        }
    }
    for child in state.executions.values_mut() {
        let in_run = child
            .map_run_arn
            .as_ref()
            .is_some_and(|arn| aborted.contains(arn));
        if in_run && child.status == ExecutionStatus::Running {
            child.status = ExecutionStatus::Aborted;
            child.stop_date = Some(now);
        }
    }
}
