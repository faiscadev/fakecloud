//! Athena JSON 1.1 service.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use parking_lot::RwLock;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_aws::arn::Arn;
use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_glue::SharedGlueState;
use fakecloud_persistence::SnapshotStore;
use fakecloud_s3::SharedS3State;

use crate::sql::{self, ExecutedQuery};
use crate::state::{
    AccountState, AthenaAccounts, AthenaSnapshot, Calculation, CapacityAssignmentConfiguration,
    CapacityReservation, DataCatalog, NamedQuery, Notebook, PreparedStatement, QueryExecution,
    Session, SharedAthenaState, WorkGroup, ATHENA_SNAPSHOT_SCHEMA_VERSION,
};

/// Actions that mutate persisted Athena state and therefore must trigger a
/// snapshot write. Read-only actions (Batch*Get/Get*/List*, ExportNotebook,
/// CreatePresignedNotebookUrl, GetResourceDashboard) are excluded. Query
/// results are written to S3 (persisted by the S3 store); the QueryExecution
/// metadata persists here.
const MUTATING_ACTIONS: &[&str] = &[
    "CancelCapacityReservation",
    "CreateCapacityReservation",
    "CreateDataCatalog",
    "CreateNamedQuery",
    "CreateNotebook",
    "CreatePreparedStatement",
    "CreateWorkGroup",
    "DeleteCapacityReservation",
    "DeleteDataCatalog",
    "DeleteNamedQuery",
    "DeleteNotebook",
    "DeletePreparedStatement",
    "DeleteWorkGroup",
    "ImportNotebook",
    "PutCapacityAssignmentConfiguration",
    "StartCalculationExecution",
    "StartQueryExecution",
    "StartSession",
    "StopCalculationExecution",
    "StopQueryExecution",
    "TagResource",
    "TerminateSession",
    "UntagResource",
    "UpdateCapacityReservation",
    "UpdateDataCatalog",
    "UpdateNamedQuery",
    "UpdateNotebook",
    "UpdateNotebookMetadata",
    "UpdatePreparedStatement",
    "UpdateWorkGroup",
];

const SUPPORTED_ACTIONS: &[&str] = &[
    "BatchGetNamedQuery",
    "BatchGetPreparedStatement",
    "BatchGetQueryExecution",
    "CancelCapacityReservation",
    "CreateCapacityReservation",
    "CreateDataCatalog",
    "CreateNamedQuery",
    "CreateNotebook",
    "CreatePreparedStatement",
    "CreatePresignedNotebookUrl",
    "CreateWorkGroup",
    "DeleteCapacityReservation",
    "DeleteDataCatalog",
    "DeleteNamedQuery",
    "DeleteNotebook",
    "DeletePreparedStatement",
    "DeleteWorkGroup",
    "ExportNotebook",
    "GetCalculationExecution",
    "GetCalculationExecutionCode",
    "GetCalculationExecutionStatus",
    "GetCapacityAssignmentConfiguration",
    "GetCapacityReservation",
    "GetDatabase",
    "GetDataCatalog",
    "GetNamedQuery",
    "GetNotebookMetadata",
    "GetPreparedStatement",
    "GetQueryExecution",
    "GetQueryResults",
    "GetQueryRuntimeStatistics",
    "GetResourceDashboard",
    "GetSession",
    "GetSessionEndpoint",
    "GetSessionStatus",
    "GetTableMetadata",
    "GetWorkGroup",
    "ImportNotebook",
    "ListApplicationDPUSizes",
    "ListCalculationExecutions",
    "ListCapacityReservations",
    "ListDatabases",
    "ListDataCatalogs",
    "ListEngineVersions",
    "ListExecutors",
    "ListNamedQueries",
    "ListNotebookMetadata",
    "ListNotebookSessions",
    "ListPreparedStatements",
    "ListQueryExecutions",
    "ListSessions",
    "ListTableMetadata",
    "ListTagsForResource",
    "ListWorkGroups",
    "PutCapacityAssignmentConfiguration",
    "StartCalculationExecution",
    "StartQueryExecution",
    "StartSession",
    "StopCalculationExecution",
    "StopQueryExecution",
    "TagResource",
    "TerminateSession",
    "UntagResource",
    "UpdateCapacityReservation",
    "UpdateDataCatalog",
    "UpdateNamedQuery",
    "UpdateNotebook",
    "UpdateNotebookMetadata",
    "UpdatePreparedStatement",
    "UpdateWorkGroup",
];

pub struct AthenaService {
    state: SharedAthenaState,
    glue: Option<SharedGlueState>,
    s3: Option<SharedS3State>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

mod capacity;
mod data_catalogs;
mod lineage;
mod metadata;
mod named_queries;
mod notebooks;
mod prepared_statements;
mod queries;
mod tags;
mod work_groups;

impl AthenaService {
    pub fn new(state: SharedAthenaState) -> Self {
        Self {
            state,
            glue: None,
            s3: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_glue(mut self, glue: SharedGlueState) -> Self {
        self.glue = Some(glue);
        self
    }

    pub fn with_s3(mut self, s3: SharedS3State) -> Self {
        self.s3 = Some(s3);
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn shared_state(&self) -> SharedAthenaState {
        Arc::clone(&self.state)
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes, with serde
    /// + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_athena_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Athena state when invoked, or
    /// `None` in memory mode. The CloudFormation provisioner mutates `state`
    /// directly and uses this to write a CFN-provisioned resource through to
    /// disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_athena_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current Athena state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `AthenaService::save_snapshot` and the
/// CloudFormation provisioner persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_athena_snapshot(
    state: &SharedAthenaState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = AthenaSnapshot {
        schema_version: ATHENA_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write athena snapshot"),
        Err(err) => tracing::error!(%err, "athena snapshot task panicked"),
    }
}

impl Default for AthenaService {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(AthenaAccounts::new())))
    }
}

#[async_trait]
impl AwsService for AthenaService {
    fn service_name(&self) -> &str {
        "athena"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = MUTATING_ACTIONS.contains(&req.action.as_str());
        let result = match req.action.as_str() {
            // Workgroups
            "CreateWorkGroup" => self.create_work_group(&req),
            "GetWorkGroup" => self.get_work_group(&req),
            "ListWorkGroups" => self.list_work_groups(&req),
            "UpdateWorkGroup" => self.update_work_group(&req),
            "DeleteWorkGroup" => self.delete_work_group(&req),

            // Data catalogs
            "CreateDataCatalog" => self.create_data_catalog(&req),
            "GetDataCatalog" => self.get_data_catalog(&req),
            "ListDataCatalogs" => self.list_data_catalogs(&req),
            "UpdateDataCatalog" => self.update_data_catalog(&req),
            "DeleteDataCatalog" => self.delete_data_catalog(&req),
            "GetDatabase" => self.get_database(&req),
            "ListDatabases" => self.list_databases(&req),
            "GetTableMetadata" => self.get_table_metadata(&req),
            "ListTableMetadata" => self.list_table_metadata(&req),

            // Named queries
            "CreateNamedQuery" => self.create_named_query(&req),
            "GetNamedQuery" => self.get_named_query(&req),
            "ListNamedQueries" => self.list_named_queries(&req),
            "BatchGetNamedQuery" => self.batch_get_named_query(&req),
            "UpdateNamedQuery" => self.update_named_query(&req),
            "DeleteNamedQuery" => self.delete_named_query(&req),

            // Prepared statements
            "CreatePreparedStatement" => self.create_prepared_statement(&req),
            "GetPreparedStatement" => self.get_prepared_statement(&req),
            "ListPreparedStatements" => self.list_prepared_statements(&req),
            "BatchGetPreparedStatement" => self.batch_get_prepared_statement(&req),
            "UpdatePreparedStatement" => self.update_prepared_statement(&req),
            "DeletePreparedStatement" => self.delete_prepared_statement(&req),

            // Query executions
            "StartQueryExecution" => self.start_query_execution(&req),
            "StopQueryExecution" => self.stop_query_execution(&req),
            "GetQueryExecution" => self.get_query_execution(&req),
            "ListQueryExecutions" => self.list_query_executions(&req),
            "BatchGetQueryExecution" => self.batch_get_query_execution(&req),
            "GetQueryResults" => self.get_query_results(&req),
            "GetQueryRuntimeStatistics" => self.get_query_runtime_statistics(&req),

            // Notebooks
            "CreateNotebook" => self.create_notebook(&req),
            "ImportNotebook" => self.import_notebook(&req),
            "ExportNotebook" => self.export_notebook(&req),
            "GetNotebookMetadata" => self.get_notebook_metadata(&req),
            "ListNotebookMetadata" => self.list_notebook_metadata(&req),
            "UpdateNotebook" => self.update_notebook(&req),
            "UpdateNotebookMetadata" => self.update_notebook_metadata(&req),
            "DeleteNotebook" => self.delete_notebook(&req),
            "CreatePresignedNotebookUrl" => self.create_presigned_notebook_url(&req),

            // Sessions / calculations
            "StartSession" => self.start_session(&req),
            "GetSession" => self.get_session(&req),
            "GetSessionStatus" => self.get_session_status(&req),
            "GetSessionEndpoint" => self.get_session_endpoint(&req),
            "ListSessions" => self.list_sessions(&req),
            "ListNotebookSessions" => self.list_notebook_sessions(&req),
            "TerminateSession" => self.terminate_session(&req),
            "StartCalculationExecution" => self.start_calculation_execution(&req),
            "StopCalculationExecution" => self.stop_calculation_execution(&req),
            "GetCalculationExecution" => self.get_calculation_execution(&req),
            "GetCalculationExecutionCode" => self.get_calculation_execution_code(&req),
            "GetCalculationExecutionStatus" => self.get_calculation_execution_status(&req),
            "ListCalculationExecutions" => self.list_calculation_executions(&req),

            // Capacity reservations
            "CreateCapacityReservation" => self.create_capacity_reservation(&req),
            "GetCapacityReservation" => self.get_capacity_reservation(&req),
            "ListCapacityReservations" => self.list_capacity_reservations(&req),
            "UpdateCapacityReservation" => self.update_capacity_reservation(&req),
            "CancelCapacityReservation" => self.cancel_capacity_reservation(&req),
            "DeleteCapacityReservation" => self.delete_capacity_reservation(&req),
            "PutCapacityAssignmentConfiguration" => {
                self.put_capacity_assignment_configuration(&req)
            }
            "GetCapacityAssignmentConfiguration" => {
                self.get_capacity_assignment_configuration(&req)
            }

            // Tags
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),

            // Misc / read-only catalog
            "ListEngineVersions" => self.list_engine_versions(&req),
            "ListApplicationDPUSizes" => self.list_application_dpu_sizes(&req),
            "ListExecutors" => self.list_executors(&req),
            "GetResourceDashboard" => self.get_resource_dashboard(&req),

            other => Err(AwsServiceError::action_not_implemented("athena", other)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}

// ─── Workgroups ────────────────────────────────────────────────────

impl AthenaService {}

// ─── Data catalogs ─────────────────────────────────────────────────

impl AthenaService {}

// ─── Named queries ─────────────────────────────────────────────────

impl AthenaService {}

// ─── Prepared statements ───────────────────────────────────────────

impl AthenaService {}

// ─── Query executions ──────────────────────────────────────────────

impl AthenaService {}

// ─── Notebooks ─────────────────────────────────────────────────────

impl AthenaService {}

// ─── Sessions / calculations ──────────────────────────────────────

impl AthenaService {}

// ─── Capacity reservations ────────────────────────────────────────

impl AthenaService {}

// ─── Tags / misc ──────────────────────────────────────────────────

impl AthenaService {
    fn list_engine_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: MaxResults targets MaxEngineVersionsCount @range(1,10);
        // NextToken targets Token @length(1,1024).
        let max = validate_max_results(&body, 1, 10)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let all = vec![
            json!({"EffectiveEngineVersion": "Athena engine version 3", "SelectedEngineVersion": "AUTO"}),
            json!({"EffectiveEngineVersion": "Athena engine version 3", "SelectedEngineVersion": "Athena engine version 3"}),
        ];
        // Honor MaxResults + NextToken round-tripping instead of always
        // returning the full array (bug-audit 2026-06-13, 1.10).
        let (page, next) =
            paginate_checked(&all, body.get("NextToken").and_then(Value::as_str), max)
                .map_err(|_| invalid_request("Invalid NextToken"))?;
        let mut resp = json!({ "EngineVersions": page });
        if let Some(t) = next {
            resp["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    fn list_application_dpu_sizes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: MaxResults targets MaxApplicationDPUSizesCount @range(1,100);
        // NextToken targets Token @length(1,1024).
        let max = validate_max_results(&body, 1, 100)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let all = vec![
            json!({"ApplicationRuntimeId": "Athena-PySpark-3.0", "SupportedDPUSizes": [1, 2, 4, 8, 16, 32]}),
        ];
        let (page, next) =
            paginate_checked(&all, body.get("NextToken").and_then(Value::as_str), max)
                .map_err(|_| invalid_request("Invalid NextToken"))?;
        let mut resp = json!({ "ApplicationDPUSizes": page });
        if let Some(t) = next {
            resp["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    fn list_executors(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let session_id = require_str(&body, "SessionId")?;
        // Smithy: MaxResults targets MaxListExecutorsCount @range(1,100);
        // NextToken targets SessionManagerToken @length(1,1024).
        let max = validate_max_results(&body, 1, 100)?;
        validate_opt_string_len(&body, "NextToken", 1, 1024)?;
        let state_filter = body.get("ExecutorStateFilter").and_then(Value::as_str);

        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let session = account
            .sessions
            .get(&session_id)
            .ok_or_else(|| invalid_request(format!("Session {session_id} not found")))?;

        // A live Spark session has a coordinator executor. A terminated
        // session reports it as TERMINATED. Derive the executor from the
        // session state instead of always returning an empty list
        // (bug-audit 2026-06-13, 1.10).
        let session_active = matches!(session.state.as_str(), "IDLE" | "BUSY" | "CREATED");
        let executor_state = if session_active {
            "CREATED"
        } else {
            "TERMINATED"
        };
        let start = session.start_date_time.timestamp_millis();
        let termination = session.end_date_time.map(|d| json!(d.timestamp_millis()));
        let mut executor = json!({
            "ExecutorId": format!("{session_id}-coordinator"),
            "ExecutorType": "COORDINATOR",
            "ExecutorState": executor_state,
            "ExecutorSize": 1i64,
            "StartDateTime": start,
        });
        if let Some(t) = termination {
            executor["TerminationDateTime"] = t;
        }

        let all: Vec<Value> = match state_filter {
            Some(f) if f != executor_state => Vec::new(),
            _ => vec![executor],
        };
        let (page, next) =
            paginate_checked(&all, body.get("NextToken").and_then(Value::as_str), max)
                .map_err(|_| invalid_request("Invalid NextToken"))?;
        let mut resp = json!({
            "SessionId": session_id,
            "ExecutorsSummary": page,
        });
        if let Some(t) = next {
            resp["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }
}

// ─── Helpers ───────────────────────────────────────────────────────

fn account_mut<'a>(state: &'a mut AthenaAccounts, account_id: &str) -> &'a mut AccountState {
    let a = state.accounts.entry(account_id.to_string()).or_default();
    a.ensure_initialized();
    a
}

fn require_str(body: &Value, field: &str) -> Result<String, AwsServiceError> {
    body.get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| invalid_request(format!("{field} is required")))
}

/// Validate `MaxResults` is within [min, max] when present. AWS Athena
/// uses range constraints like @range(min: 1, max: 50); the probe sends
/// boundary-violating values and expects InvalidRequestException.
fn validate_max_results(body: &Value, min: i64, max: i64) -> Result<usize, AwsServiceError> {
    if let Some(v) = body.get("MaxResults") {
        if let Some(n) = v.as_i64() {
            if n < min || n > max {
                return Err(invalid_request(format!(
                    "MaxResults value {n} is outside the valid range {min}-{max}",
                )));
            }
            return Ok(n as usize);
        }
    }
    Ok(50)
}

/// Validate a string body field length is within [min, max] when
/// present. Used for `@length`-constrained inputs like `ClientRequestToken`.
fn validate_opt_string_len(
    body: &Value,
    field: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    if let Some(Value::String(s)) = body.get(field) {
        let len = s.chars().count();
        if len < min || len > max {
            return Err(invalid_request(format!(
                "{field} length {len} is outside the valid range {min}-{max}",
            )));
        }
    }
    Ok(())
}

/// Require that a string field be present and within `[min, max]` characters.
/// Combines existence and `@length` enforcement for required `@length`-constrained
/// fields like `Name` / `Database` / `QueryString`.
fn validate_required_string_len(
    body: &Value,
    field: &str,
    min: usize,
    max: usize,
) -> Result<String, AwsServiceError> {
    let s = body
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_request(format!("{field} is required")))?;
    let len = s.chars().count();
    if len < min || len > max {
        return Err(invalid_request(format!(
            "{field} length {len} is outside the valid range {min}-{max}",
        )));
    }
    Ok(s.to_string())
}

/// Validate that a list-typed field is present (non-null) and non-empty when
/// the Smithy model marks it `@required` with `@length(min: 1, ...)`. Used for
/// batch operations like `BatchGetNamedQuery` where the list itself is required.
fn validate_required_list(body: &Value, field: &str) -> Result<(), AwsServiceError> {
    match body.get(field) {
        Some(Value::Array(_)) => Ok(()),
        _ => Err(invalid_request(format!("{field} is required"))),
    }
}

/// Validate that a list-typed field, if present, has a length within `[min, max]`.
fn validate_list_len(
    body: &Value,
    field: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    if let Some(Value::Array(a)) = body.get(field) {
        let len = a.len();
        if len < min || len > max {
            return Err(invalid_request(format!(
                "{field} length {len} is outside the valid range {min}-{max}",
            )));
        }
    }
    Ok(())
}

/// Validate an optional enum field's value against the allowed set.
fn validate_opt_enum(body: &Value, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(Value::String(s)) = body.get(field) {
        if !allowed.contains(&s.as_str()) {
            return Err(invalid_request(format!(
                "{field} value {s:?} is not a valid enum value",
            )));
        }
    }
    Ok(())
}

/// Replace `?` placeholders in a query string with the provided parameter
/// values, quoted safely for SQL parsing.
fn substitute_parameters(query: &str, params: &[Value]) -> Result<String, AwsServiceError> {
    let mut result = String::with_capacity(query.len());
    let mut param_iter = params.iter().filter_map(Value::as_str);
    let mut chars = query.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            // Skip inside string literals so we don't replace ? in strings.
            result.push('\'');
            while let Some(ch) = chars.next() {
                result.push(ch);
                if ch == '\'' {
                    // Handle escaped quotes ('').
                    if chars.peek() == Some(&'\'') {
                        result.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
            }
        } else if c == '?' {
            let p = param_iter
                .next()
                .ok_or_else(|| invalid_request("More placeholders than ExecutionParameters"))?;
            let escaped = p.replace('\'', "''");
            result.push('\'');
            result.push_str(&escaped);
            result.push('\'');
        } else {
            result.push(c);
        }
    }
    Ok(result)
}

fn invalid_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequestException", msg)
}

fn synth_uuid() -> String {
    Uuid::new_v4().to_string()
}

fn workgroup_arn(account_id: &str, region: &str, name: &str) -> String {
    let region = if region.is_empty() {
        "us-east-1"
    } else {
        region
    };
    Arn::new("athena", region, account_id, &format!("workgroup/{name}")).to_string()
}

fn datacatalog_arn(account_id: &str, region: &str, name: &str) -> String {
    let region = if region.is_empty() {
        "us-east-1"
    } else {
        region
    };
    Arn::new("athena", region, account_id, &format!("datacatalog/{name}")).to_string()
}

fn capacity_reservation_arn(account_id: &str, region: &str, name: &str) -> String {
    let region = if region.is_empty() {
        "us-east-1"
    } else {
        region
    };
    Arn::new(
        "athena",
        region,
        account_id,
        &format!("capacity-reservation/{name}"),
    )
    .to_string()
}

fn parse_string_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|v| {
            v.iter()
                .filter_map(|s| s.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_tags(
    value: Option<&Value>,
) -> Result<std::collections::BTreeMap<String, String>, AwsServiceError> {
    let mut out = std::collections::BTreeMap::new();
    let Some(arr) = value.and_then(Value::as_array) else {
        return Ok(out);
    };
    for tag in arr {
        let key = tag
            .get("Key")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_request("Tag.Key is required"))?
            .to_string();
        let value = tag
            .get("Value")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        out.insert(key, value);
    }
    Ok(out)
}

/// Map the leading SQL keyword to one of Athena's documented statement types.
fn classify_statement(query: &str) -> String {
    let trimmed = query.trim_start();
    let upper = trimmed.to_uppercase();
    if upper.starts_with("SELECT")
        || upper.starts_with("SHOW")
        || upper.starts_with("DESCRIBE")
        || upper.starts_with("EXPLAIN")
    {
        "DML".to_string()
    } else if upper.starts_with("CREATE") || upper.starts_with("ALTER") || upper.starts_with("DROP")
    {
        "DDL".to_string()
    } else if upper.starts_with("UPDATE")
        || upper.starts_with("INSERT")
        || upper.starts_with("DELETE")
    {
        "DML".to_string()
    } else {
        "UTILITY".to_string()
    }
}

// ─── JSON shaping ──────────────────────────────────────────────────

fn work_group_json(wg: &WorkGroup) -> Value {
    let mut obj = json!({
        "Name": wg.name,
        "State": wg.state,
        "CreationTime": wg.creation_time.timestamp() as f64,
    });
    if let Some(d) = &wg.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    if let Some(c) = &wg.configuration {
        obj.as_object_mut()
            .unwrap()
            .insert("Configuration".to_string(), c.clone());
    }
    obj
}

fn workgroup_summary_json(wg: &WorkGroup) -> Value {
    let mut obj = json!({
        "Name": wg.name,
        "State": wg.state,
        "CreationTime": wg.creation_time.timestamp() as f64,
    });
    if let Some(d) = &wg.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    if let Some(v) = &wg.engine_version {
        obj.as_object_mut().unwrap().insert(
            "EngineVersion".to_string(),
            json!({"SelectedEngineVersion": v}),
        );
    }
    obj
}

fn data_catalog_json(c: &DataCatalog) -> Value {
    let mut obj = json!({
        "Name": c.name,
        "Type": c.cat_type,
        "Status": c.status,
        "Parameters": c.parameters,
    });
    if let Some(d) = &c.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    if let Some(t) = &c.connection_type {
        obj.as_object_mut()
            .unwrap()
            .insert("ConnectionType".to_string(), Value::String(t.clone()));
    }
    if let Some(e) = &c.error {
        obj.as_object_mut()
            .unwrap()
            .insert("Error".to_string(), Value::String(e.clone()));
    }
    obj
}

fn named_query_json(q: &NamedQuery) -> Value {
    let mut obj = json!({
        "Name": q.name,
        "Database": q.database,
        "QueryString": q.query_string,
        "NamedQueryId": q.named_query_id,
        "WorkGroup": q.work_group,
    });
    if let Some(d) = &q.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    obj
}

fn prepared_statement_json(p: &PreparedStatement) -> Value {
    let mut obj = json!({
        "StatementName": p.statement_name,
        "QueryStatement": p.query_statement,
        "WorkGroupName": p.work_group_name,
        "LastModifiedTime": p.last_modified_time.timestamp() as f64,
    });
    if let Some(d) = &p.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    obj
}

fn query_execution_json(q: &QueryExecution) -> Value {
    let mut obj = json!({
        "QueryExecutionId": q.query_execution_id,
        "Query": q.query,
        "StatementType": q.statement_type,
        "WorkGroup": q.work_group,
        "Status": {
            "State": q.state,
            "SubmissionDateTime": q.submission_time.timestamp() as f64,
            "CompletionDateTime": q.completion_time.map(|t| t.timestamp() as f64),
        },
        "Statistics": {
            "DataScannedInBytes": q.data_scanned_bytes,
            "EngineExecutionTimeInMillis": q.engine_execution_time_ms,
            "QueryPlanningTimeInMillis": q.query_planning_time_ms,
            "TotalExecutionTimeInMillis": q.total_execution_time_ms,
        },
    });
    if let Some(c) = &q.query_execution_context {
        obj.as_object_mut()
            .unwrap()
            .insert("QueryExecutionContext".to_string(), c.clone());
    }
    if let Some(c) = &q.result_configuration {
        obj.as_object_mut()
            .unwrap()
            .insert("ResultConfiguration".to_string(), c.clone());
    }
    if let Some(v) = &q.engine_version {
        obj.as_object_mut()
            .unwrap()
            .insert("EngineVersion".to_string(), v.clone());
    }
    if let Some(reason) = &q.state_change_reason {
        obj["Status"].as_object_mut().unwrap().insert(
            "StateChangeReason".to_string(),
            Value::String(reason.clone()),
        );
    }
    obj
}

fn notebook_metadata_json(n: &Notebook) -> Value {
    json!({
        "NotebookId": n.notebook_id,
        "Name": n.name,
        "WorkGroup": n.work_group,
        "Type": n.notebook_type,
        "CreationTime": n.creation_time.timestamp() as f64,
        "LastModifiedTime": n.last_modified_time.timestamp() as f64,
    })
}

fn session_summary_json(s: &Session) -> Value {
    json!({
        "SessionId": s.session_id,
        "Description": s.description,
        "Status": session_status_json(s),
        "EngineVersion": s.engine_version,
        "NotebookVersion": s.notebook_version,
    })
}

fn session_detail_json(s: &Session) -> Value {
    let mut obj = json!({
        "SessionId": s.session_id,
        "WorkGroup": s.work_group,
        "Status": session_status_json(s),
        "EngineVersion": s.engine_version,
        "NotebookVersion": s.notebook_version,
    });
    if let Some(d) = &s.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    if let Some(c) = &s.configuration {
        obj.as_object_mut()
            .unwrap()
            .insert("EngineConfiguration".to_string(), c.clone());
    }
    obj
}

fn session_status_json(s: &Session) -> Value {
    let mut obj = json!({
        "State": s.state,
        "StartDateTime": s.start_date_time.timestamp() as f64,
    });
    if let Some(t) = s.end_date_time {
        obj.as_object_mut()
            .unwrap()
            .insert("EndDateTime".to_string(), json!(t.timestamp() as f64));
    }
    if let Some(t) = s.idle_since_date_time {
        obj.as_object_mut()
            .unwrap()
            .insert("IdleSinceDateTime".to_string(), json!(t.timestamp() as f64));
    }
    obj
}

fn calculation_summary_json(c: &Calculation) -> Value {
    json!({
        "CalculationExecutionId": c.calculation_execution_id,
        "Description": c.description,
        "Status": calculation_status_json(c),
    })
}

fn calculation_detail_json(c: &Calculation) -> Value {
    let mut obj = json!({
        "CalculationExecutionId": c.calculation_execution_id,
        "SessionId": c.session_id,
        "Status": calculation_status_json(c),
    });
    if let Some(d) = &c.description {
        obj.as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(d.clone()));
    }
    // Only advertise a Result when a result object was actually written (a real,
    // resolvable ResultS3Uri). A COMPLETED calculation with no written result
    // carries no Result block rather than a dangling URI.
    if let (Some(w), Some(rt)) = (&c.working_directory, &c.result_type) {
        obj.as_object_mut().unwrap().insert(
            "Result".to_string(),
            json!({ "ResultS3Uri": w, "ResultType": rt }),
        );
    }
    obj
}

fn calculation_status_json(c: &Calculation) -> Value {
    let mut obj = json!({
        "State": c.state,
        "SubmissionDateTime": c.submission_date_time.timestamp() as f64,
    });
    if let Some(t) = c.completion_date_time {
        obj.as_object_mut().unwrap().insert(
            "CompletionDateTime".to_string(),
            json!(t.timestamp() as f64),
        );
    }
    if let Some(reason) = &c.state_change_reason {
        obj.as_object_mut().unwrap().insert(
            "StateChangeReason".to_string(),
            Value::String(reason.clone()),
        );
    }
    obj
}

fn capacity_reservation_json(cr: &CapacityReservation) -> Value {
    let mut obj = json!({
        "Name": cr.name,
        "Status": cr.status,
        "TargetDpus": cr.target_dpus,
        "AllocatedDpus": cr.allocated_dpus,
        "CreationTime": cr.creation_time.timestamp() as f64,
    });
    if let Some(t) = cr.last_allocation {
        obj.as_object_mut().unwrap().insert(
            "LastAllocation".to_string(),
            json!({
                "Status": "SUCCEEDED",
                "RequestTime": t.timestamp() as f64,
            }),
        );
    }
    if let Some(t) = cr.last_successful_allocation_time {
        obj.as_object_mut().unwrap().insert(
            "LastSuccessfulAllocationTime".to_string(),
            json!(t.timestamp() as f64),
        );
    }
    obj
}

// ─── Glue -> Athena helpers ────────────────────────────────────────

fn glue_database_json(db: &fakecloud_glue::Database) -> Value {
    let mut obj = json!({
        "Name": db.name,
        "Description": db.description.as_deref().unwrap_or(""),
        "Parameters": db.parameters,
    });
    if let Some(uri) = &db.location_uri {
        obj.as_object_mut()
            .unwrap()
            .insert("LocationUri".to_string(), Value::String(uri.clone()));
    }
    obj
}

fn glue_table_metadata_json(tbl: &fakecloud_glue::Table) -> Value {
    let columns: Vec<Value> = tbl
        .storage_descriptor
        .as_ref()
        .map(|s| {
            s.columns
                .iter()
                .map(|c| {
                    let mut col = json!({
                        "Name": c.name,
                        "Type": c.column_type,
                    });
                    if let Some(comment) = &c.comment {
                        col.as_object_mut()
                            .unwrap()
                            .insert("Comment".to_string(), Value::String(comment.clone()));
                    }
                    col
                })
                .collect()
        })
        .unwrap_or_default();

    let partition_keys: Vec<Value> = tbl
        .partition_keys
        .iter()
        .map(|c| {
            let mut col = json!({
                "Name": c.name,
                "Type": c.column_type,
            });
            if let Some(comment) = &c.comment {
                col.as_object_mut()
                    .unwrap()
                    .insert("Comment".to_string(), Value::String(comment.clone()));
            }
            col
        })
        .collect();

    let mut obj = json!({
        "Name": tbl.name,
        "TableType": tbl.table_type.as_deref().unwrap_or("EXTERNAL_TABLE"),
        "Columns": columns,
        "PartitionKeys": partition_keys,
    });

    let params = &tbl.parameters;
    if !params.is_empty() {
        obj.as_object_mut().unwrap().insert(
            "Parameters".to_string(),
            serde_json::to_value(params).unwrap(),
        );
    }
    obj
}

fn match_table_expression(name: &str, expression: &str) -> bool {
    if expression == "*" {
        return true;
    }
    regex::Regex::new(&format!("^{expression}$")).is_ok_and(|re| re.is_match(name))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;
    use fakecloud_core::service::AwsRequest;
    use fakecloud_glue::Column;
    use fakecloud_glue::{Database, GlueAccounts, SharedGlueState, StorageDescriptor, Table};
    use parking_lot::RwLock;
    use serde_json::json;

    use crate::service::{substitute_parameters, AthenaService};
    use crate::state::AthenaAccounts;
    use crate::SharedAthenaState;

    fn test_service() -> (AthenaService, SharedGlueState) {
        let glue = Arc::new(RwLock::new(GlueAccounts::new()));
        let svc = AthenaService::new(Arc::new(RwLock::new(AthenaAccounts::new())))
            .with_glue(Arc::clone(&glue));
        (svc, glue)
    }

    fn seed_glue_db(glue: &SharedGlueState, account_id: &str, region: &str, db: Database) {
        let mut g = glue.write();
        let acct = g.get_or_create(account_id, region);
        let dbs = acct.dbs_in_mut(region);
        dbs.insert(db.name.clone(), db);
    }

    fn seed_glue_table(
        glue: &SharedGlueState,
        account_id: &str,
        region: &str,
        db_name: &str,
        table: Table,
    ) {
        let mut g = glue.write();
        let acct = g.get_or_create(account_id, region);
        let dbs = acct.dbs_in_mut(region);
        let db = dbs.get_mut(db_name).unwrap();
        db.tables.insert(table.name.clone(), table);
    }

    fn make_db(name: &str) -> Database {
        Database {
            name: name.to_string(),
            description: Some(format!("db {name}")),
            location_uri: Some(format!("s3://bucket/{name}")),
            parameters: std::collections::BTreeMap::new(),
            created_at: Utc::now(),
            catalog_id: "123456789012".to_string(),
            target_database: None,
            federated_database: None,
            tables: std::collections::BTreeMap::new(),
        }
    }

    fn make_table(name: &str, db_name: &str) -> Table {
        Table {
            name: name.to_string(),
            database_name: db_name.to_string(),
            description: Some(format!("table {name}")),
            owner: None,
            create_time: Utc::now(),
            update_time: Utc::now(),
            last_access_time: None,
            retention: 0,
            storage_descriptor: Some(StorageDescriptor {
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        column_type: "int".to_string(),
                        comment: Some("pk".to_string()),
                        parameters: std::collections::BTreeMap::new(),
                    },
                    Column {
                        name: "name".to_string(),
                        column_type: "string".to_string(),
                        comment: None,
                        parameters: std::collections::BTreeMap::new(),
                    },
                ],
                location: Some("s3://bucket/data/".to_string()),
                input_format: None,
                output_format: None,
                compressed: None,
                serde_info: None,
                parameters: std::collections::BTreeMap::new(),
                bucket_columns: vec![],
                number_of_buckets: None,
                stored_as_sub_directories: None,
                sort_columns: vec![],
                skewed_info: None,
            }),
            partition_keys: vec![],
            view_original_text: None,
            view_expanded_text: None,
            table_type: Some("EXTERNAL_TABLE".to_string()),
            parameters: std::collections::BTreeMap::new(),
            partitions: std::collections::BTreeMap::new(),
            catalog_id: "123456789012".to_string(),
        }
    }

    fn req(action: &str, body: serde_json::Value) -> AwsRequest {
        AwsRequest {
            service: "athena".to_string(),
            action: action.to_string(),
            body: serde_json::to_vec(&body).unwrap().into(),
            query_params: Default::default(),
            headers: Default::default(),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "req-1".to_string(),
            principal: None,
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    fn parse_json(resp: &fakecloud_core::service::AwsResponse) -> serde_json::Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    #[test]
    fn get_query_results_paginates_large_result_set() {
        // Regression: GetQueryResults honors MaxResults + NextToken. Before
        // the fix it returned only the first MaxResults rows and emitted no
        // NextToken, leaving the rest of a large result set unreachable.
        let svc = AthenaService::new(SharedAthenaState::default());
        let id = "qe-paginate";
        {
            let mut state = svc.state.write();
            let account = super::account_mut(&mut state, "123456789012");
            let now = Utc::now();
            account.query_executions.insert(
                id.to_string(),
                crate::state::QueryExecution {
                    query_execution_id: id.to_string(),
                    query: "SELECT n FROM t".to_string(),
                    statement_type: "DML".to_string(),
                    work_group: "primary".to_string(),
                    state: "SUCCEEDED".to_string(),
                    state_change_reason: None,
                    submission_time: now,
                    completion_time: Some(now),
                    query_execution_context: None,
                    result_configuration: None,
                    engine_version: None,
                    data_scanned_bytes: 0,
                    engine_execution_time_ms: 1,
                    query_planning_time_ms: 1,
                    total_execution_time_ms: 2,
                    result_rows: (0..5).map(|n| vec![n.to_string()]).collect(),
                    result_columns: vec![("n".to_string(), "integer".to_string())],
                },
            );
        }

        let mut data_rows: Vec<String> = Vec::new();
        let mut next: Option<String> = None;
        let mut pages = 0;
        loop {
            pages += 1;
            let mut body = json!({ "QueryExecutionId": id, "MaxResults": 3 });
            if let Some(t) = &next {
                body["NextToken"] = json!(t);
            }
            let resp = svc
                .get_query_results(&req("GetQueryResults", body))
                .unwrap();
            let parsed = parse_json(&resp);
            let rows = parsed["ResultSet"]["Rows"].as_array().unwrap();
            // The column-header row is emitted only on the first page.
            let start = if pages == 1 { 1 } else { 0 };
            for row in &rows[start..] {
                data_rows.push(row["Data"][0]["VarCharValue"].as_str().unwrap().to_string());
            }
            match parsed.get("NextToken").and_then(|v| v.as_str()) {
                Some(t) => next = Some(t.to_string()),
                None => break,
            }
            assert!(pages < 10, "pagination did not terminate");
        }

        assert!(pages >= 2, "large result set must span multiple pages");
        assert_eq!(data_rows, vec!["0", "1", "2", "3", "4"]);

        // MaxResults=1 (header consumes the only slot on page 1) must still
        // terminate and surface every data row rather than loop forever.
        let mut data_rows: Vec<String> = Vec::new();
        let mut next: Option<String> = None;
        let mut pages = 0;
        loop {
            pages += 1;
            let mut body = json!({ "QueryExecutionId": id, "MaxResults": 1 });
            if let Some(t) = &next {
                body["NextToken"] = json!(t);
            }
            let resp = svc
                .get_query_results(&req("GetQueryResults", body))
                .unwrap();
            let parsed = parse_json(&resp);
            let rows = parsed["ResultSet"]["Rows"].as_array().unwrap();
            let start = if pages == 1 { 1 } else { 0 };
            for row in &rows[start..] {
                data_rows.push(row["Data"][0]["VarCharValue"].as_str().unwrap().to_string());
            }
            match parsed.get("NextToken").and_then(|v| v.as_str()) {
                Some(t) => next = Some(t.to_string()),
                None => break,
            }
            assert!(pages < 100, "MaxResults=1 pagination did not terminate");
        }
        assert_eq!(data_rows, vec!["0", "1", "2", "3", "4"]);
    }

    #[test]
    fn substitute_parameters_replaces_placeholders() {
        let out = substitute_parameters(
            "SELECT * FROM t WHERE id = ? AND name = ?",
            &[json!("42"), json!("Alice")],
        )
        .unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE id = '42' AND name = 'Alice'");
    }

    #[test]
    fn substitute_parameters_skips_placeholders_in_string_literals() {
        let out = substitute_parameters(
            "SELECT * FROM t WHERE name = 'foo?bar' AND id = ?",
            &[json!("7")],
        )
        .unwrap();
        assert_eq!(out, "SELECT * FROM t WHERE name = 'foo?bar' AND id = '7'");
    }

    #[test]
    fn substitute_parameters_errors_on_too_few_params() {
        let err =
            substitute_parameters("SELECT * WHERE a = ? AND b = ?", &[json!("1")]).unwrap_err();
        assert!(err
            .message()
            .contains("More placeholders than ExecutionParameters"));
    }

    #[test]
    fn substitute_parameters_escapes_quotes() {
        let out = substitute_parameters("SELECT * WHERE name = ?", &[json!("O'Brien")]).unwrap();
        assert_eq!(out, "SELECT * WHERE name = 'O''Brien'");
    }

    #[test]
    fn update_work_group_merges_configuration_updates() {
        // UpdateWorkGroup stored ConfigurationUpdates verbatim, corrupting
        // Configuration's shape (bug-audit 2026-06-20, 1.24). It must merge the
        // updates with the right field mapping and preserve untouched fields.
        let svc = AthenaService::new(SharedAthenaState::default());
        svc.create_work_group(&req(
            "CreateWorkGroup",
            json!({
                "Name": "wg",
                "Configuration": {
                    "ResultConfiguration": {"OutputLocation": "s3://orig/"},
                    "PublishCloudWatchMetricsEnabled": false
                }
            }),
        ))
        .unwrap();

        svc.update_work_group(&req(
            "UpdateWorkGroup",
            json!({
                "WorkGroup": "wg",
                "ConfigurationUpdates": {
                    "PublishCloudWatchMetricsEnabled": true,
                    "ResultConfigurationUpdates": {"OutputLocation": "s3://new/"}
                }
            }),
        ))
        .unwrap();

        let got = parse_json(
            &svc.get_work_group(&req("GetWorkGroup", json!({ "WorkGroup": "wg" })))
                .unwrap(),
        );
        let cfg = &got["WorkGroup"]["Configuration"];
        // ResultConfigurationUpdates merged into ResultConfiguration (right shape).
        assert_eq!(
            cfg["ResultConfiguration"]["OutputLocation"], "s3://new/",
            "{cfg}"
        );
        // Scalar update applied; no stray ConfigurationUpdates/ResultConfigurationUpdates keys.
        assert_eq!(cfg["PublishCloudWatchMetricsEnabled"], true);
        assert!(cfg.get("ResultConfigurationUpdates").is_none(), "{cfg}");
    }

    #[test]
    fn start_query_execution_with_named_query_id() {
        let svc = AthenaService::new(SharedAthenaState::default());
        let create_resp = svc
            .create_named_query(&req(
                "CreateNamedQuery",
                json!({
                    "Name": "my-query",
                    "Database": "default",
                    "QueryString": "SELECT 1 AS id",
                    "WorkGroup": "primary"
                }),
            ))
            .unwrap();
        let nq_id = parse_json(&create_resp)
            .get("NamedQueryId")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        let exec_resp = svc
            .start_query_execution(&req(
                "StartQueryExecution",
                json!({ "NamedQueryId": nq_id }),
            ))
            .unwrap();
        let body = parse_json(&exec_resp);
        assert!(body.get("QueryExecutionId").is_some());

        let qe_id = body.get("QueryExecutionId").unwrap().as_str().unwrap();
        let qe_resp = svc
            .get_query_execution(&req(
                "GetQueryExecution",
                json!({ "QueryExecutionId": qe_id }),
            ))
            .unwrap();
        let qe = parse_json(&qe_resp).get("QueryExecution").unwrap().clone();
        assert_eq!(qe.get("Query").unwrap(), "SELECT 1 AS id");
    }

    #[test]
    fn query_records_workgroup_engine_version() {
        // Regression: StartQueryExecution stored a hardcoded AUTO / v3
        // EngineVersion, ignoring the workgroup's pinned version.
        let svc = AthenaService::new(SharedAthenaState::default());
        svc.create_work_group(&req(
            "CreateWorkGroup",
            json!({
                "Name": "v2wg",
                "Configuration": {
                    "EngineVersion": { "SelectedEngineVersion": "Athena engine version 2" }
                }
            }),
        ))
        .unwrap();

        let exec = parse_json(
            &svc.start_query_execution(&req(
                "StartQueryExecution",
                json!({ "QueryString": "SELECT 1 AS id", "WorkGroup": "v2wg" }),
            ))
            .unwrap(),
        );
        let qe_id = exec["QueryExecutionId"].as_str().unwrap().to_string();
        let qe = parse_json(
            &svc.get_query_execution(&req(
                "GetQueryExecution",
                json!({ "QueryExecutionId": qe_id }),
            ))
            .unwrap(),
        );
        let ev = &qe["QueryExecution"]["EngineVersion"];
        assert_eq!(ev["SelectedEngineVersion"], "Athena engine version 2");
        assert_eq!(ev["EffectiveEngineVersion"], "Athena engine version 2");

        // The default primary workgroup pins nothing, so it falls back to AUTO/v3.
        let exec2 = parse_json(
            &svc.start_query_execution(&req(
                "StartQueryExecution",
                json!({ "QueryString": "SELECT 1 AS id", "WorkGroup": "primary" }),
            ))
            .unwrap(),
        );
        let qe2_id = exec2["QueryExecutionId"].as_str().unwrap().to_string();
        let qe2 = parse_json(
            &svc.get_query_execution(&req(
                "GetQueryExecution",
                json!({ "QueryExecutionId": qe2_id }),
            ))
            .unwrap(),
        );
        let ev2 = &qe2["QueryExecution"]["EngineVersion"];
        assert_eq!(ev2["SelectedEngineVersion"], "AUTO");
        assert_eq!(ev2["EffectiveEngineVersion"], "Athena engine version 3");
    }

    #[test]
    fn calculation_execution_advertises_no_dangling_result() {
        // Regression: StartCalculationExecution fabricated a COMPLETED state with
        // a dangling s3://athena-calc-results/{uuid} ResultS3Uri and reported a
        // hardcoded DpuExecutionInMillis:100. With no result location configured
        // no Result block is advertised, and Statistics come from the record.
        let svc = AthenaService::new(SharedAthenaState::default());
        let sess = parse_json(
            &svc.start_session(&req("StartSession", json!({ "WorkGroup": "primary" })))
                .unwrap(),
        );
        let session_id = sess["SessionId"].as_str().unwrap().to_string();

        let calc = parse_json(
            &svc.start_calculation_execution(&req(
                "StartCalculationExecution",
                json!({ "SessionId": session_id, "CodeBlock": "print(1)" }),
            ))
            .unwrap(),
        );
        let calc_id = calc["CalculationExecutionId"].as_str().unwrap().to_string();

        let detail = parse_json(
            &svc.get_calculation_execution(&req(
                "GetCalculationExecution",
                json!({ "CalculationExecutionId": calc_id }),
            ))
            .unwrap(),
        );
        assert_eq!(detail["Status"]["State"], "COMPLETED");
        assert!(
            detail.get("Result").is_none(),
            "no dangling ResultS3Uri should be advertised, got {:?}",
            detail.get("Result")
        );

        let status = parse_json(
            &svc.get_calculation_execution_status(&req(
                "GetCalculationExecutionStatus",
                json!({ "CalculationExecutionId": calc_id }),
            ))
            .unwrap(),
        );
        assert_eq!(status["Statistics"]["DpuExecutionInMillis"], 0);
        assert_eq!(status["Statistics"]["Progress"], "100%");
    }

    #[test]
    fn start_query_execution_bumps_named_query_last_used_at() {
        let svc = AthenaService::new(SharedAthenaState::default());
        let create_resp = svc
            .create_named_query(&req(
                "CreateNamedQuery",
                json!({
                    "Name": "tracked",
                    "Database": "default",
                    "QueryString": "SELECT 1",
                    "WorkGroup": "primary"
                }),
            ))
            .unwrap();
        let nq_id = parse_json(&create_resp)
            .get("NamedQueryId")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        // Before any StartQueryExecution the named query has no last_used_at.
        {
            let state = svc.state.read();
            let account = state.accounts.values().next().unwrap();
            let nq = account.named_queries.get(&nq_id).unwrap();
            assert!(nq.last_used_at.is_none());
        }

        svc.start_query_execution(&req(
            "StartQueryExecution",
            json!({ "NamedQueryId": nq_id }),
        ))
        .unwrap();

        let state = svc.state.read();
        let account = state.accounts.values().next().unwrap();
        let nq = account.named_queries.get(&nq_id).unwrap();
        assert!(
            nq.last_used_at.is_some(),
            "last_used_at must be populated after StartQueryExecution",
        );
    }

    #[test]
    fn start_query_execution_with_execution_parameters() {
        let svc = AthenaService::new(SharedAthenaState::default());
        let exec_resp = svc
            .start_query_execution(&req(
                "StartQueryExecution",
                json!({
                    "QueryString": "SELECT ? AS id, ? AS name",
                    "ExecutionParameters": ["42", "Alice"]
                }),
            ))
            .unwrap();
        let body = parse_json(&exec_resp);
        let qe_id = body.get("QueryExecutionId").unwrap().as_str().unwrap();
        let qe_resp = svc
            .get_query_execution(&req(
                "GetQueryExecution",
                json!({ "QueryExecutionId": qe_id }),
            ))
            .unwrap();
        let qe = parse_json(&qe_resp).get("QueryExecution").unwrap().clone();
        assert_eq!(
            qe.get("Query").unwrap(),
            "SELECT '42' AS id, 'Alice' AS name"
        );
    }

    #[test]
    fn list_databases_reads_glue() {
        let (svc, glue) = test_service();
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("sales"));
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("inventory"));

        let resp = svc
            .list_databases(&req(
                "ListDatabases",
                json!({ "CatalogName": "AwsDataCatalog" }),
            ))
            .unwrap();
        let body = parse_json(&resp);
        let list = body.get("DatabaseList").unwrap().as_array().unwrap();
        assert_eq!(list.len(), 2);
        let names: Vec<String> = list
            .iter()
            .map(|d| d.get("Name").unwrap().as_str().unwrap().to_string())
            .collect();
        assert!(names.contains(&"sales".to_string()));
        assert!(names.contains(&"inventory".to_string()));
    }

    #[test]
    fn get_database_reads_glue() {
        let (svc, glue) = test_service();
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("sales"));

        let resp = svc
            .get_database(&req(
                "GetDatabase",
                json!({ "CatalogName": "AwsDataCatalog", "DatabaseName": "sales" }),
            ))
            .unwrap();
        let body = parse_json(&resp);
        let db = body.get("Database").unwrap();
        assert_eq!(db.get("Name").unwrap(), "sales");
        assert_eq!(
            db.get("LocationUri").unwrap().as_str().unwrap(),
            "s3://bucket/sales"
        );
    }

    #[test]
    fn start_query_execution_missing_named_query_errors() {
        let svc = AthenaService::new(SharedAthenaState::default());
        svc.create_named_query(&req(
            "CreateNamedQuery",
            json!({
                "Name": "seed",
                "Database": "default",
                "QueryString": "SELECT 1",
                "WorkGroup": "primary"
            }),
        ))
        .unwrap();
        match svc.start_query_execution(&req(
            "StartQueryExecution",
            json!({ "NamedQueryId": "nope" }),
        )) {
            Ok(_) => panic!("expected error"),
            Err(err) => assert!(err.message().contains("NamedQuery nope not found")),
        }
    }

    #[test]
    fn get_database_missing_in_glue_errors() {
        let (svc, glue) = test_service();
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("sales"));

        let err = match svc.get_database(&req(
            "GetDatabase",
            json!({ "CatalogName": "AwsDataCatalog", "DatabaseName": "missing" }),
        )) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(err.message().contains("Database missing not found"));
    }

    #[test]
    fn list_table_metadata_reads_glue() {
        let (svc, glue) = test_service();
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("sales"));
        seed_glue_table(
            &glue,
            "123456789012",
            "us-east-1",
            "sales",
            make_table("orders", "sales"),
        );
        seed_glue_table(
            &glue,
            "123456789012",
            "us-east-1",
            "sales",
            make_table("returns", "sales"),
        );

        let resp = svc
            .list_table_metadata(&req(
                "ListTableMetadata",
                json!({ "CatalogName": "AwsDataCatalog", "DatabaseName": "sales" }),
            ))
            .unwrap();
        let body = parse_json(&resp);
        let list = body.get("TableMetadataList").unwrap().as_array().unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn get_table_metadata_reads_glue() {
        let (svc, glue) = test_service();
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("sales"));
        seed_glue_table(
            &glue,
            "123456789012",
            "us-east-1",
            "sales",
            make_table("orders", "sales"),
        );

        let resp = svc
            .get_table_metadata(&req(
                "GetTableMetadata",
                json!({
                    "CatalogName": "AwsDataCatalog",
                    "DatabaseName": "sales",
                    "TableName": "orders"
                }),
            ))
            .unwrap();
        let body = parse_json(&resp);
        let meta = body.get("TableMetadata").unwrap();
        assert_eq!(meta.get("Name").unwrap(), "orders");
        let cols = meta.get("Columns").unwrap().as_array().unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].get("Name").unwrap(), "id");
        assert_eq!(cols[0].get("Type").unwrap(), "int");
    }

    #[test]
    fn get_table_metadata_missing_errors() {
        let (svc, glue) = test_service();
        seed_glue_db(&glue, "123456789012", "us-east-1", make_db("sales"));

        let err = match svc.get_table_metadata(&req(
            "GetTableMetadata",
            json!({
                "CatalogName": "AwsDataCatalog",
                "DatabaseName": "sales",
                "TableName": "missing"
            }),
        )) {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(err.message().contains("Table sales.missing not found"));
    }

    #[test]
    fn match_table_expression_wildcard() {
        use super::match_table_expression;
        assert!(match_table_expression("orders", "*"));
        assert!(match_table_expression("orders", "ord.*"));
        assert!(!match_table_expression("orders", "ret.*"));
        assert!(match_table_expression("orders", "ord.rs"));
        assert!(!match_table_expression("orders", "ord.r"));
        // Exact expression must match the name, not every name.
        assert!(match_table_expression("orders", "orders"));
        assert!(!match_table_expression("orders", "sales"));
    }

    #[test]
    fn list_engine_versions_paginates_with_next_token() {
        // MaxResults + NextToken are honored, not ignored (1.10).
        let svc = AthenaService::new(SharedAthenaState::default());
        let resp = svc
            .list_engine_versions(&req("ListEngineVersions", json!({ "MaxResults": 1 })))
            .unwrap();
        let body = parse_json(&resp);
        assert_eq!(body["EngineVersions"].as_array().unwrap().len(), 1);
        let token = body["NextToken"].as_str().expect("expected a NextToken");

        let resp = svc
            .list_engine_versions(&req(
                "ListEngineVersions",
                json!({ "MaxResults": 1, "NextToken": token }),
            ))
            .unwrap();
        let body = parse_json(&resp);
        assert_eq!(body["EngineVersions"].as_array().unwrap().len(), 1);
        // Two total engine versions -> second page exhausts the set.
        assert!(body["NextToken"].is_null());
    }

    #[test]
    fn list_engine_versions_rejects_bad_next_token() {
        let svc = AthenaService::new(SharedAthenaState::default());
        let err = match svc.list_engine_versions(&req(
            "ListEngineVersions",
            json!({ "NextToken": "garbage" }),
        )) {
            Err(e) => e,
            Ok(_) => panic!("expected an error for a non-numeric NextToken"),
        };
        assert!(err.message().contains("Invalid NextToken"));
    }

    #[test]
    fn list_executors_reflects_active_session() {
        // An active session reports a coordinator executor rather than an
        // unconditional empty list (1.10).
        let svc = AthenaService::new(SharedAthenaState::default());
        let resp = svc
            .start_session(&req("StartSession", json!({ "WorkGroup": "primary" })))
            .unwrap();
        let session_id = parse_json(&resp)["SessionId"].as_str().unwrap().to_string();

        let resp = svc
            .list_executors(&req("ListExecutors", json!({ "SessionId": session_id })))
            .unwrap();
        let body = parse_json(&resp);
        let executors = body["ExecutorsSummary"].as_array().unwrap();
        assert_eq!(executors.len(), 1);
        assert_eq!(executors[0]["ExecutorType"], json!("COORDINATOR"));
        assert_eq!(executors[0]["ExecutorState"], json!("CREATED"));
    }

    // ListWorkGroups' summary EngineVersion reflects the workgroup's real
    // stored EngineVersion rather than a hardcoded "AUTO".
    #[test]
    fn list_work_groups_reports_real_engine_version() {
        let svc = AthenaService::default();
        svc.create_work_group(&req(
            "CreateWorkGroup",
            json!({
                "Name": "wg3",
                "Configuration": {
                    "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3"}
                }
            }),
        ))
        .unwrap();

        let find_engine = |svc: &AthenaService, name: &str| -> String {
            let body = parse_json(
                &svc.list_work_groups(&req("ListWorkGroups", json!({})))
                    .unwrap(),
            );
            body["WorkGroups"]
                .as_array()
                .unwrap()
                .iter()
                .find(|w| w["Name"] == json!(name))
                .and_then(|w| w["EngineVersion"]["SelectedEngineVersion"].as_str())
                .unwrap()
                .to_string()
        };
        assert_eq!(find_engine(&svc, "wg3"), "Athena engine version 3");

        // Updating the engine version via ConfigurationUpdates is reflected too.
        svc.update_work_group(&req(
            "UpdateWorkGroup",
            json!({
                "WorkGroup": "wg3",
                "ConfigurationUpdates": {
                    "EngineVersion": {"SelectedEngineVersion": "Athena engine version 2"}
                }
            }),
        ))
        .unwrap();
        assert_eq!(find_engine(&svc, "wg3"), "Athena engine version 2");
    }
}

#[cfg(test)]
mod pagination_reject_test {
    #[test]
    fn paginate_checked_rejects_invalid_token() {
        use fakecloud_core::pagination::paginate_checked;
        let items: Vec<i32> = (0..5).collect();
        assert!(paginate_checked(&items, Some("bad"), 3).is_err());
        assert!(paginate_checked(&items, Some("2"), 3).is_ok());
    }
}
