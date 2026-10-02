//! `AthenaService` `queries` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl AthenaService {
    pub(super) fn start_query_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: ClientRequestToken targets IdempotencyToken @length(32,128).
        validate_opt_string_len(&body, "ClientRequestToken", 32, 128)?;
        // QueryString @length(1,262144) is enforced below if the caller provides it directly.
        if let Some(Value::String(s)) = body.get("QueryString") {
            let len = s.chars().count();
            if !(1..=262144).contains(&len) {
                return Err(invalid_request(format!(
                    "QueryString length {len} is outside the valid range 1-262144",
                )));
            }
        }
        let work_group = body
            .get("WorkGroup")
            .and_then(Value::as_str)
            .unwrap_or("primary")
            .to_string();
        let context = body.get("QueryExecutionContext").cloned();
        let result_configuration = body.get("ResultConfiguration").cloned();
        let default_database = context
            .as_ref()
            .and_then(|c| c.get("Database"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let output_location = result_configuration
            .as_ref()
            .and_then(|c| c.get("OutputLocation"))
            .and_then(Value::as_str)
            .map(str::to_owned);

        // Resolve query string from NamedQueryId or QueryString.
        let mut query = if let Some(named_query_id) =
            body.get("NamedQueryId").and_then(Value::as_str)
        {
            let mut state = self.state.write();
            let account = state
                .accounts
                .get_mut(&req.account_id)
                .ok_or_else(|| invalid_request(format!("NamedQuery {named_query_id} not found")))?;
            let nq = account
                .named_queries
                .get_mut(named_query_id)
                .ok_or_else(|| invalid_request(format!("NamedQuery {named_query_id} not found")))?;
            nq.last_used_at = Some(Utc::now());
            nq.query_string.clone()
        } else {
            require_str(&body, "QueryString")?
        };

        // Apply ExecutionParameters substitution.
        if let Some(params) = body.get("ExecutionParameters").and_then(Value::as_array) {
            query = substitute_parameters(&query, params)?;
        }

        // Workgroup existence check before kicking off SQL execution so we
        // surface the same error users hit on real Athena. Resolve the query's
        // EngineVersion from the workgroup it runs in (the workgroup's
        // Configuration/EngineVersion, set at Create/UpdateWorkGroup) rather than
        // hardcoding AUTO/v3.
        let (engine_version, wg_output_location, enforce_wg) = {
            let mut state = self.state.write();
            let account = account_mut(&mut state, &req.account_id);
            match account.work_groups.get(&work_group) {
                Some(wg) => {
                    let cfg = wg.configuration.as_ref();
                    let wg_out = cfg
                        .and_then(|c| c.pointer("/ResultConfiguration/OutputLocation"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    let enforce = cfg
                        .and_then(|c| c.get("EnforceWorkGroupConfiguration"))
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    (resolve_engine_version(wg), wg_out, enforce)
                }
                None => {
                    return Err(invalid_request(format!("Workgroup {work_group} not found")));
                }
            }
        };

        // Effective result location: the workgroup's own ResultConfiguration.
        // OutputLocation is consulted when the request omits one, and OVERRIDES
        // the request when EnforceWorkGroupConfiguration is set -- matching real
        // Athena, where the common pattern is to configure OutputLocation once on
        // the workgroup and omit it per query. Without this the result S3 object
        // was never written and GetQueryExecution echoed an empty location.
        let output_location = if enforce_wg {
            wg_output_location.clone().or(output_location)
        } else {
            output_location.or_else(|| wg_output_location.clone())
        };

        let id = synth_uuid();
        let now = Utc::now();

        // Echo the resolved OutputLocation back in ResultConfiguration so
        // GetQueryExecution reports where results go from the moment the query
        // is accepted (real Athena always does). Once the query finishes it
        // points at the exact `<OutputLocation>/<QueryExecutionId>.csv` key.
        let mut effective_result_config = result_configuration.clone();
        if let Some(out) = output_location.clone() {
            if let Some(obj) = effective_result_config
                .get_or_insert_with(|| json!({}))
                .as_object_mut()
            {
                obj.insert("OutputLocation".to_string(), Value::String(out));
            }
        }

        // Record the query as QUEUED and answer with its id straight away, as
        // Athena does. The SQL runs as a job (S3 scan, result write) that
        // GetQueryExecution reports moving QUEUED -> RUNNING -> SUCCEEDED or
        // FAILED; clients poll for it.
        {
            let mut state = self.state.write();
            let account = account_mut(&mut state, &req.account_id);
            account.query_executions.insert(
                id.clone(),
                QueryExecution {
                    query_execution_id: id.clone(),
                    query: query.clone(),
                    statement_type: classify_statement(&query),
                    work_group,
                    state: "QUEUED".to_string(),
                    state_change_reason: None,
                    submission_time: now,
                    completion_time: None,
                    query_execution_context: context,
                    result_configuration: effective_result_config,
                    engine_version: Some(engine_version),
                    data_scanned_bytes: 0,
                    engine_execution_time_ms: 0,
                    query_planning_time_ms: 0,
                    total_execution_time_ms: 0,
                    result_rows: Vec::new(),
                    result_columns: Vec::new(),
                    region: Some(req.region.clone()),
                },
            );
        }
        self.spawn_query(QueryJob {
            account_id: req.account_id.clone(),
            region: req.region.clone(),
            query_execution_id: id.clone(),
            query,
            default_database,
            output_location,
        });
        Ok(AwsResponse::ok_json(json!({ "QueryExecutionId": id })))
    }

    /// Run a query job off the request path: on the blocking pool when a Tokio
    /// runtime is available (then persist the outcome), inline otherwise.
    fn spawn_query(&self, job: QueryJob) {
        let env = QueryJobEnv {
            state: self.state.clone(),
            glue: self.glue.clone(),
            s3: self.s3.clone(),
            s3_store: self.s3_store.clone(),
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            run_query_job(&env, &job);
            return;
        };
        let state = self.state.clone();
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        handle.spawn(async move {
            if let Err(err) = tokio::task::spawn_blocking(move || run_query_job(&env, &job)).await {
                tracing::error!(%err, "athena query job panicked");
            }
            save_athena_snapshot(&state, store, &lock).await;
        });
    }

    /// Re-run queries a previous process accepted but never finished, so they
    /// do not report QUEUED/RUNNING forever after a restart.
    pub fn resume_interrupted_queries(&self) {
        let jobs: Vec<QueryJob> = {
            let mut state = self.state.write();
            let mut jobs = Vec::new();
            for (account_id, account) in state.accounts.iter_mut() {
                for q in account.query_executions.values_mut() {
                    if q.state != "QUEUED" && q.state != "RUNNING" {
                        continue;
                    }
                    q.state = "QUEUED".to_string();
                    let ctx_db = q
                        .query_execution_context
                        .as_ref()
                        .and_then(|c| c.get("Database"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    let out = q
                        .result_configuration
                        .as_ref()
                        .and_then(|c| c.get("OutputLocation"))
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                    jobs.push(QueryJob {
                        account_id: account_id.clone(),
                        region: q.region.clone().unwrap_or_else(|| "us-east-1".to_string()),
                        query_execution_id: q.query_execution_id.clone(),
                        query: q.query.clone(),
                        default_database: ctx_db,
                        output_location: out,
                    });
                }
            }
            jobs
        };
        for job in jobs {
            self.spawn_query(job);
        }
    }

    pub(super) fn stop_query_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "QueryExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let q = account
            .query_executions
            .get_mut(&id)
            .ok_or_else(|| invalid_request(format!("QueryExecution {id} not found")))?;
        // Stopping a query that already finished is a no-op on Athena; an
        // in-flight one is cancelled and its job discards its outcome.
        if q.state == "QUEUED" || q.state == "RUNNING" {
            q.state = "CANCELLED".to_string();
            q.state_change_reason = Some("Query cancelled by user".to_string());
            q.completion_time = Some(Utc::now());
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_query_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "QueryExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let q = account
            .query_executions
            .get(&id)
            .ok_or_else(|| invalid_request(format!("QueryExecution {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "QueryExecution": query_execution_json(q),
        })))
    }

    pub(super) fn list_query_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let work_group = body
            .get("WorkGroup")
            .and_then(Value::as_str)
            .unwrap_or("primary")
            .to_string();
        // Smithy: MaxResults targets MaxQueryExecutionsCount @range(0,50);
        // NextToken targets Token @length(1,1024).
        let max_results = validate_max_results(&body, 0, 50)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut all: Vec<QueryExecution> = account
            .query_executions
            .values()
            .filter(|q| q.work_group == work_group)
            .cloned()
            .collect();
        all.sort_by_key(|q| std::cmp::Reverse(q.submission_time));
        let ids: Vec<String> = all.iter().map(|q| q.query_execution_id.clone()).collect();
        let (page, next) = paginate_checked(&ids, next_token.as_deref(), max_results)
            .map_err(|_| invalid_request("Invalid NextToken"))?;
        let mut response = json!({ "QueryExecutionIds": page });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn batch_get_query_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: @required QueryExecutionIds (@length min:1 max:50).
        validate_required_list(&body, "QueryExecutionIds")?;
        validate_list_len(&body, "QueryExecutionIds", 1, 50)?;
        let ids = parse_string_list(body.get("QueryExecutionIds"));
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let mut found = Vec::new();
        let mut missing = Vec::new();
        for id in ids {
            if let Some(q) = account.query_executions.get(&id) {
                found.push(query_execution_json(q));
            } else {
                missing.push(json!({
                    "QueryExecutionId": id,
                    "ErrorCode": "NOT_FOUND",
                    "ErrorMessage": "QueryExecution not found",
                }));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "QueryExecutions": found,
            "UnprocessedQueryExecutionIds": missing,
        })))
    }

    pub(super) fn get_query_results(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "QueryExecutionId")?;
        // Smithy: MaxResults targets MaxRowsCount @range(1,1000);
        // NextToken targets Token @length(1,1024).
        let max_results = validate_max_results(&body, 1, 1000)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let next_token = body
            .get("NextToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let q = account
            .query_executions
            .get(&id)
            .ok_or_else(|| invalid_request(format!("QueryExecution {id} not found")))?;
        if q.state != "SUCCEEDED" {
            let msg = match q.state.as_str() {
                "QUEUED" | "RUNNING" => {
                    format!("Query has not yet finished. Current state: {}", q.state)
                }
                _ => format!(
                    "Query did not finish successfully. Final query state: {}",
                    q.state
                ),
            };
            return Err(invalid_request(msg));
        }
        let column_info: Vec<Value> = q
            .result_columns
            .iter()
            .map(|(name, ty)| {
                json!({
                    "CatalogName": "AwsDataCatalog",
                    "SchemaName": "default",
                    "TableName": "",
                    "Name": name,
                    "Label": name,
                    "Type": ty,
                    "Precision": 0,
                    "Scale": 0,
                    "Nullable": "NULLABLE",
                    "CaseSensitive": false,
                })
            })
            .collect();
        // Pagination: the header row (column names) is emitted only on the
        // first page (NextToken absent) and counts against MaxResults there.
        // The NextToken is a numeric offset into the data rows; subsequent
        // pages carry data rows only. Without this, result sets larger than
        // MaxResults were unreachable past page 1 (bug-hunt 2026-07-16).
        let offset: usize = match next_token.as_deref() {
            None => 0,
            Some(tok) => tok
                .parse()
                .map_err(|_| invalid_request("Invalid NextToken"))?,
        };
        let total = q.result_rows.len();
        let start = offset.min(total);
        // The column-header row is emitted only on the first invocation (no
        // NextToken supplied). Keying this on token absence rather than
        // `offset == 0` avoids an infinite loop when MaxResults is 1: the
        // first page returns only the header with NextToken "0", and the
        // resumed page must then advance past offset 0 rather than treating
        // itself as the first page again.
        let include_header = next_token.is_none();
        let data_budget = if include_header {
            max_results.saturating_sub(1)
        } else {
            max_results
        };
        let end = start.saturating_add(data_budget).min(total);

        let mut rows = Vec::new();
        if include_header {
            rows.push(json!({
                "Data": q.result_columns.iter().map(|(n, _)| json!({"VarCharValue": n})).collect::<Vec<_>>(),
            }));
        }
        for row in &q.result_rows[start..end] {
            rows.push(json!({
                "Data": row.iter().map(|v| json!({"VarCharValue": v})).collect::<Vec<_>>(),
            }));
        }
        let mut response = json!({
            "ResultSet": {
                "Rows": rows,
                "ResultSetMetadata": {"ColumnInfo": column_info},
            },
            "UpdateCount": 0,
        });
        if end < total {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextToken".to_string(), Value::String(end.to_string()));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn get_query_runtime_statistics(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = require_str(&body, "QueryExecutionId")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let q = account
            .query_executions
            .get(&id)
            .ok_or_else(|| invalid_request(format!("QueryExecution {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({
            "QueryRuntimeStatistics": {
                "Timeline": {
                    "QueryQueueTimeInMillis": 0,
                    "QueryPlanningTimeInMillis": q.query_planning_time_ms,
                    "EngineExecutionTimeInMillis": q.engine_execution_time_ms,
                    "ServiceProcessingTimeInMillis": 0,
                    "TotalExecutionTimeInMillis": q.total_execution_time_ms,
                },
                "Rows": {
                    "InputRows": q.result_rows.len() as i64,
                    "InputBytes": q.data_scanned_bytes,
                    "OutputRows": q.result_rows.len() as i64,
                    "OutputBytes": q.data_scanned_bytes,
                },
            }
        })))
    }
}

/// Resolve the `EngineVersion` a query should run under from the workgroup it
/// runs in. Athena pins the effective engine version to the workgroup's
/// configured `EngineVersion`; only when the workgroup pins nothing does it fall
/// back to AUTO / "Athena engine version 3".
fn resolve_engine_version(wg: &WorkGroup) -> Value {
    // Prefer the EngineVersion object stored in the workgroup Configuration.
    if let Some(ev) = wg
        .configuration
        .as_ref()
        .and_then(|c| c.get("EngineVersion"))
        .filter(|v| v.is_object())
    {
        let selected = ev
            .get("SelectedEngineVersion")
            .and_then(Value::as_str)
            .unwrap_or("AUTO");
        let effective = ev
            .get("EffectiveEngineVersion")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| effective_engine_version(selected));
        return json!({
            "SelectedEngineVersion": selected,
            "EffectiveEngineVersion": effective,
        });
    }
    // Fall back to the summary's SelectedEngineVersion string.
    if let Some(selected) = wg.engine_version.as_deref() {
        return json!({
            "SelectedEngineVersion": selected,
            "EffectiveEngineVersion": effective_engine_version(selected),
        });
    }
    json!({
        "SelectedEngineVersion": "AUTO",
        "EffectiveEngineVersion": "Athena engine version 3",
    })
}

/// The effective engine version for a `SelectedEngineVersion`: an explicit pin
/// is its own effective version; AUTO resolves to the current default.
fn effective_engine_version(selected: &str) -> String {
    if selected.eq_ignore_ascii_case("AUTO") {
        "Athena engine version 3".to_string()
    } else {
        selected.to_string()
    }
}

/// Everything a query job needs to run without the request.
pub(super) struct QueryJob {
    pub account_id: String,
    pub region: String,
    pub query_execution_id: String,
    pub query: String,
    pub default_database: Option<String>,
    pub output_location: Option<String>,
}

pub(super) struct QueryJobEnv {
    pub state: SharedAthenaState,
    pub glue: Option<SharedGlueState>,
    pub s3: Option<SharedS3State>,
    pub s3_store: Option<Arc<dyn fakecloud_persistence::S3Store>>,
}

/// Move `id` from `from` to RUNNING; false when the query is gone or no
/// longer in `from` (e.g. StopQueryExecution cancelled it).
fn begin_query(state: &SharedAthenaState, account_id: &str, id: &str) -> bool {
    let mut st = state.write();
    let Some(q) = st
        .accounts
        .get_mut(account_id)
        .and_then(|a| a.query_executions.get_mut(id))
    else {
        return false;
    };
    if q.state != "QUEUED" {
        return false;
    }
    q.state = "RUNNING".to_string();
    true
}

/// Execute a query job: QUEUED -> RUNNING, run the SQL (S3 reads and the
/// result write happen with no Athena or S3 lock held), then record the
/// outcome unless the query was cancelled meanwhile.
pub(super) fn run_query_job(env: &QueryJobEnv, job: &QueryJob) {
    if !begin_query(&env.state, &job.account_id, &job.query_execution_id) {
        return;
    }
    let started = std::time::Instant::now();
    let executed = sql::execute(
        &job.query,
        job.default_database.as_deref(),
        job.output_location.as_deref(),
        &sql::ExecEnv {
            account_id: &job.account_id,
            region: &job.region,
            glue: env.glue.as_ref(),
            s3: env.s3.as_ref(),
            s3_store: env.s3_store.as_deref(),
            query_execution_id: &job.query_execution_id,
        },
    );
    let elapsed_ms = started.elapsed().as_millis() as i64;

    let mut st = env.state.write();
    let Some(q) = st
        .accounts
        .get_mut(&job.account_id)
        .and_then(|a| a.query_executions.get_mut(&job.query_execution_id))
    else {
        return;
    };
    if q.state != "RUNNING" {
        // Cancelled while running: keep CANCELLED and drop the outcome.
        return;
    }
    q.completion_time = Some(Utc::now());
    q.engine_execution_time_ms = elapsed_ms;
    q.total_execution_time_ms = elapsed_ms;
    match executed {
        Ok(ExecutedQuery {
            columns,
            rows,
            data_scanned_bytes,
            output_location,
        }) => {
            q.state = "SUCCEEDED".to_string();
            q.result_columns = columns;
            q.result_rows = rows;
            q.data_scanned_bytes = data_scanned_bytes;
            if let Some(out) = output_location {
                if let Some(obj) = q
                    .result_configuration
                    .get_or_insert_with(|| json!({}))
                    .as_object_mut()
                {
                    obj.insert("OutputLocation".to_string(), Value::String(out));
                }
            }
        }
        Err(err) => {
            tracing::debug!(query = %job.query, error = %err, "athena: query failed");
            q.state = "FAILED".to_string();
            q.state_change_reason = Some(err.to_string());
        }
    }
}
