//! Amazon Timestream awsJson1_0 dispatch + operation handlers.
//!
//! Requests carry `X-Amz-Target: Timestream_20181101.<Operation>` for BOTH the
//! Timestream Write and Timestream Query SDK clients (they share the target
//! prefix); dispatch keys off `req.action`. Every operation runs model-driven
//! input validation first, then real, account-partitioned, persisted behavior
//! over one shared store: databases and tables created on the write side are
//! read back by `Query` on the query side.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::query;
use crate::shared::{
    database_arn, endpoint_address, new_id, now_epoch, scheduled_query_arn, table_arn, table_key,
};
use crate::state::{
    AccountSettings, BatchLoadTask, Database, ScheduledQuery, SharedTimestreamState, StoredRecord,
    Table, TimestreamData, MAX_RECORDS_PER_TABLE,
};

/// Every operation name across the Timestream Write + Query Smithy models
/// (30 distinct operations; the four shared ops appear once).
pub const TIMESTREAM_ACTIONS: &[&str] = &[
    // Write control plane
    "CreateDatabase",
    "DeleteDatabase",
    "DescribeDatabase",
    "ListDatabases",
    "UpdateDatabase",
    "CreateTable",
    "DeleteTable",
    "DescribeTable",
    "ListTables",
    "UpdateTable",
    "WriteRecords",
    "CreateBatchLoadTask",
    "DescribeBatchLoadTask",
    "ListBatchLoadTasks",
    "ResumeBatchLoadTask",
    // Query plane
    "Query",
    "CancelQuery",
    "PrepareQuery",
    "CreateScheduledQuery",
    "DeleteScheduledQuery",
    "DescribeScheduledQuery",
    "ExecuteScheduledQuery",
    "ListScheduledQueries",
    "UpdateScheduledQuery",
    "DescribeAccountSettings",
    "UpdateAccountSettings",
    // Shared
    "DescribeEndpoints",
    "ListTagsForResource",
    "TagResource",
    "UntagResource",
];

/// Read-only verbs; any other action mutates persisted state and triggers a
/// snapshot after success.
fn is_mutating_action(action: &str) -> bool {
    if action == "Query" {
        return false;
    }
    const READ_PREFIXES: &[&str] = &["Describe", "List", "Prepare", "Cancel"];
    !READ_PREFIXES.iter().any(|p| action.starts_with(p))
}

pub struct TimestreamService {
    state: SharedTimestreamState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl TimestreamService {
    pub fn new(state: SharedTimestreamState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        crate::persistence::save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    fn with_account_mut<R>(&self, req: &AwsRequest, f: impl FnOnce(&mut TimestreamData) -> R) -> R {
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        f(acct)
    }
}

#[async_trait]
impl AwsService for TimestreamService {
    fn service_name(&self) -> &str {
        "timestream"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        if let Err(msg) = crate::validate::validate_input(&action, &req.json_body()) {
            return Err(validation_exception(msg));
        }
        let result = self.dispatch(&action, &req);
        if is_mutating_action(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        TIMESTREAM_ACTIONS
    }
}

impl TimestreamService {
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        match action {
            // Databases
            "CreateDatabase" => self.create_database(req, &body),
            "DeleteDatabase" => self.delete_database(req, &body),
            "DescribeDatabase" => self.describe_database(req, &body),
            "ListDatabases" => self.list_databases(req, &body),
            "UpdateDatabase" => self.update_database(req, &body),
            // Tables
            "CreateTable" => self.create_table(req, &body),
            "DeleteTable" => self.delete_table(req, &body),
            "DescribeTable" => self.describe_table(req, &body),
            "ListTables" => self.list_tables(req, &body),
            "UpdateTable" => self.update_table(req, &body),
            // Ingestion
            "WriteRecords" => self.write_records(req, &body),
            // Batch load
            "CreateBatchLoadTask" => self.create_batch_load_task(req, &body),
            "DescribeBatchLoadTask" => self.describe_batch_load_task(req, &body),
            "ListBatchLoadTasks" => self.list_batch_load_tasks(req, &body),
            "ResumeBatchLoadTask" => self.resume_batch_load_task(req, &body),
            // Query
            "Query" => self.query(req, &body),
            "CancelQuery" => self.cancel_query(req, &body),
            "PrepareQuery" => self.prepare_query(req, &body),
            // Scheduled queries
            "CreateScheduledQuery" => self.create_scheduled_query(req, &body),
            "DeleteScheduledQuery" => self.delete_scheduled_query(req, &body),
            "DescribeScheduledQuery" => self.describe_scheduled_query(req, &body),
            "ExecuteScheduledQuery" => self.execute_scheduled_query(req, &body),
            "ListScheduledQueries" => self.list_scheduled_queries(req, &body),
            "UpdateScheduledQuery" => self.update_scheduled_query(req, &body),
            // Account settings
            "DescribeAccountSettings" => self.describe_account_settings(req),
            "UpdateAccountSettings" => self.update_account_settings(req, &body),
            // Endpoints + tagging
            "DescribeEndpoints" => self.describe_endpoints(req),
            "ListTagsForResource" => self.list_tags_for_resource(req, &body),
            "TagResource" => self.tag_resource(req, &body),
            "UntagResource" => self.untag_resource(req, &body),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                action,
            )),
        }
    }
}

// ---- error / response helpers --------------------------------------------

fn err(code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg.into())
}

fn validation_exception(msg: impl Into<String>) -> AwsServiceError {
    err("ValidationException", msg)
}

fn resource_not_found(msg: impl Into<String>) -> AwsServiceError {
    err("ResourceNotFoundException", msg)
}

fn conflict(msg: impl Into<String>) -> AwsServiceError {
    err("ConflictException", msg)
}

fn ok(value: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::ok_json(value))
}

fn empty_ok() -> Result<AwsResponse, AwsServiceError> {
    ok(json!({}))
}

// ---- small member helpers -------------------------------------------------

fn str_member<'a>(body: &'a Value, name: &str) -> Option<&'a str> {
    body.get(name).and_then(Value::as_str)
}

fn required_str(body: &Value, name: &str) -> Result<String, AwsServiceError> {
    str_member(body, name)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| validation_exception(format!("{name} is required.")))
}

/// Render an ARN-keyed tag map as a `TagList` (`[{Key,Value}]`).
fn tag_list(map: &std::collections::BTreeMap<String, String>) -> Value {
    Value::Array(
        map.iter()
            .map(|(k, v)| json!({ "Key": k, "Value": v }))
            .collect(),
    )
}

/// Merge a `TagList` body member into an ARN-keyed tag map.
fn absorb_tags(tags: Option<&Value>, into: &mut std::collections::BTreeMap<String, String>) {
    if let Some(arr) = tags.and_then(Value::as_array) {
        for t in arr {
            if let (Some(k), Some(v)) = (
                t.get("Key").and_then(Value::as_str),
                t.get("Value").and_then(Value::as_str),
            ) {
                into.insert(k.to_string(), v.to_string());
            }
        }
    }
}

impl Database {
    fn to_wire(&self) -> Value {
        let mut o = Map::new();
        o.insert("Arn".into(), json!(self.arn));
        o.insert("DatabaseName".into(), json!(self.name));
        o.insert("TableCount".into(), json!(self.table_count));
        if let Some(k) = &self.kms_key_id {
            o.insert("KmsKeyId".into(), json!(k));
        }
        o.insert("CreationTime".into(), json!(self.creation_time));
        o.insert("LastUpdatedTime".into(), json!(self.last_updated_time));
        Value::Object(o)
    }
}

impl Table {
    fn to_wire(&self) -> Value {
        let mut o = Map::new();
        o.insert("Arn".into(), json!(self.arn));
        o.insert("TableName".into(), json!(self.name));
        o.insert("DatabaseName".into(), json!(self.database_name));
        o.insert("TableStatus".into(), json!(self.status));
        o.insert(
            "RetentionProperties".into(),
            self.retention_properties.clone(),
        );
        if let Some(m) = &self.magnetic_store_write_properties {
            o.insert("MagneticStoreWriteProperties".into(), m.clone());
        }
        o.insert("Schema".into(), self.schema.clone());
        o.insert("CreationTime".into(), json!(self.creation_time));
        o.insert("LastUpdatedTime".into(), json!(self.last_updated_time));
        Value::Object(o)
    }
}

// ---- databases ------------------------------------------------------------

impl TimestreamService {
    fn create_database(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_str(body, "DatabaseName")?;
        let kms = str_member(body, "KmsKeyId").map(str::to_string);
        let region = req.region.clone();
        let account = req.account_id.clone();
        let tags = body.get("Tags").cloned();
        self.with_account_mut(req, |data| {
            if data.databases.contains_key(&name) {
                return Err(conflict(format!(
                    "Database '{name}' already exists in this account."
                )));
            }
            let now = now_epoch();
            let arn = database_arn(&region, &account, &name);
            let db = Database {
                name: name.clone(),
                arn: arn.clone(),
                kms_key_id: kms.or_else(|| {
                    Some(format!(
                        "arn:aws:kms:{region}:{account}:key/timestream-default"
                    ))
                }),
                table_count: 0,
                creation_time: now,
                last_updated_time: now,
            };
            let wire = db.to_wire();
            data.databases.insert(name.clone(), db);
            let entry = data.tags.entry(arn).or_default();
            absorb_tags(tags.as_ref(), entry);
            ok(json!({ "Database": wire }))
        })
    }

    fn describe_database(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_str(body, "DatabaseName")?;
        let guard = self.state.read();
        let data = match guard.get(&req.account_id) {
            Some(d) => d,
            None => {
                return Err(resource_not_found(format!(
                    "Database '{name}' does not exist."
                )))
            }
        };
        let db = data
            .databases
            .get(&name)
            .ok_or_else(|| resource_not_found(format!("Database '{name}' does not exist.")))?;
        ok(json!({ "Database": db.to_wire() }))
    }

    fn list_databases(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let max = body
            .get("MaxResults")
            .and_then(Value::as_u64)
            .unwrap_or(100)
            .max(1) as usize;
        let start = decode_token(str_member(body, "NextToken"));
        let guard = self.state.read();
        let dbs: Vec<Value> = guard
            .get(&req.account_id)
            .map(|d| d.databases.values().map(Database::to_wire).collect())
            .unwrap_or_default();
        let (page, next) = paginate(&dbs, start, max);
        let mut out = Map::new();
        out.insert("Databases".into(), Value::Array(page));
        if let Some(n) = next {
            out.insert("NextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }

    fn update_database(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_str(body, "DatabaseName")?;
        let kms = str_member(body, "KmsKeyId").map(str::to_string);
        self.with_account_mut(req, |data| {
            let db = data
                .databases
                .get_mut(&name)
                .ok_or_else(|| resource_not_found(format!("Database '{name}' does not exist.")))?;
            if let Some(k) = kms {
                db.kms_key_id = Some(k);
            }
            db.last_updated_time = now_epoch();
            ok(json!({ "Database": db.to_wire() }))
        })
    }

    fn delete_database(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_str(body, "DatabaseName")?;
        self.with_account_mut(req, |data| {
            if !data.databases.contains_key(&name) {
                return Err(resource_not_found(format!(
                    "Database '{name}' does not exist."
                )));
            }
            let has_tables = data.tables.keys().any(|k| {
                k.split_once('\u{1}')
                    .map(|(d, _)| d == name)
                    .unwrap_or(false)
            });
            if has_tables {
                return Err(validation_exception(format!(
                    "The database '{name}' cannot be deleted because it still has tables."
                )));
            }
            data.databases.remove(&name);
            empty_ok()
        })
    }
}

// ---- tables ---------------------------------------------------------------

fn default_retention() -> Value {
    json!({
        "MemoryStoreRetentionPeriodInHours": 6,
        "MagneticStoreRetentionPeriodInDays": 73000
    })
}

fn default_schema() -> Value {
    json!({ "CompositePartitionKey": [ { "Type": "MEASURE" } ] })
}

fn default_magnetic() -> Value {
    json!({ "EnableMagneticStoreWrites": false })
}

impl TimestreamService {
    fn create_table(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let database = required_str(body, "DatabaseName")?;
        let table = required_str(body, "TableName")?;
        let region = req.region.clone();
        let account = req.account_id.clone();
        let retention = body
            .get("RetentionProperties")
            .cloned()
            .unwrap_or_else(default_retention);
        let magnetic = body
            .get("MagneticStoreWriteProperties")
            .cloned()
            .unwrap_or_else(default_magnetic);
        let schema = body.get("Schema").cloned().unwrap_or_else(default_schema);
        let tags = body.get("Tags").cloned();
        let key = table_key(&database, &table);
        self.with_account_mut(req, |data| {
            if !data.databases.contains_key(&database) {
                return Err(resource_not_found(format!(
                    "Database '{database}' does not exist."
                )));
            }
            if data.tables.contains_key(&key) {
                return Err(conflict(format!(
                    "Table '{table}' already exists in database '{database}'."
                )));
            }
            let now = now_epoch();
            let arn = table_arn(&region, &account, &database, &table);
            let t = Table {
                name: table.clone(),
                database_name: database.clone(),
                arn: arn.clone(),
                status: "ACTIVE".to_string(),
                retention_properties: retention,
                magnetic_store_write_properties: Some(magnetic),
                schema,
                creation_time: now,
                last_updated_time: now,
            };
            let wire = t.to_wire();
            data.tables.insert(key.clone(), t);
            data.records.entry(key).or_default();
            if let Some(db) = data.databases.get_mut(&database) {
                db.table_count += 1;
                db.last_updated_time = now;
            }
            let entry = data.tags.entry(arn).or_default();
            absorb_tags(tags.as_ref(), entry);
            ok(json!({ "Table": wire }))
        })
    }

    fn describe_table(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let database = required_str(body, "DatabaseName")?;
        let table = required_str(body, "TableName")?;
        let key = table_key(&database, &table);
        let guard = self.state.read();
        let t = guard
            .get(&req.account_id)
            .and_then(|d| d.tables.get(&key))
            .ok_or_else(|| {
                resource_not_found(format!(
                    "Table '{table}' does not exist in database '{database}'."
                ))
            })?;
        ok(json!({ "Table": t.to_wire() }))
    }

    fn list_tables(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let db_filter = str_member(body, "DatabaseName").map(str::to_string);
        let max = body
            .get("MaxResults")
            .and_then(Value::as_u64)
            .unwrap_or(100)
            .max(1) as usize;
        let start = decode_token(str_member(body, "NextToken"));
        let guard = self.state.read();
        let tables: Vec<Value> = guard
            .get(&req.account_id)
            .map(|d| {
                d.tables
                    .values()
                    .filter(|t| {
                        db_filter
                            .as_ref()
                            .map(|f| &t.database_name == f)
                            .unwrap_or(true)
                    })
                    .map(Table::to_wire)
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(&tables, start, max);
        let mut out = Map::new();
        out.insert("Tables".into(), Value::Array(page));
        if let Some(n) = next {
            out.insert("NextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }

    fn update_table(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let database = required_str(body, "DatabaseName")?;
        let table = required_str(body, "TableName")?;
        let key = table_key(&database, &table);
        let retention = body.get("RetentionProperties").cloned();
        let magnetic = body.get("MagneticStoreWriteProperties").cloned();
        let schema = body.get("Schema").cloned();
        self.with_account_mut(req, |data| {
            let t = data.tables.get_mut(&key).ok_or_else(|| {
                resource_not_found(format!(
                    "Table '{table}' does not exist in database '{database}'."
                ))
            })?;
            if let Some(r) = retention {
                t.retention_properties = r;
            }
            if let Some(m) = magnetic {
                t.magnetic_store_write_properties = Some(m);
            }
            if let Some(s) = schema {
                t.schema = s;
            }
            t.last_updated_time = now_epoch();
            ok(json!({ "Table": t.to_wire() }))
        })
    }

    fn delete_table(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let database = required_str(body, "DatabaseName")?;
        let table = required_str(body, "TableName")?;
        let key = table_key(&database, &table);
        self.with_account_mut(req, |data| {
            if data.tables.remove(&key).is_none() {
                return Err(resource_not_found(format!(
                    "Table '{table}' does not exist in database '{database}'."
                )));
            }
            data.records.remove(&key);
            if let Some(db) = data.databases.get_mut(&database) {
                db.table_count = (db.table_count - 1).max(0);
                db.last_updated_time = now_epoch();
            }
            empty_ok()
        })
    }
}

// ---- ingestion ------------------------------------------------------------

fn time_unit_multiplier(unit: &str) -> Option<i128> {
    match unit {
        "MILLISECONDS" => Some(1_000_000),
        "SECONDS" => Some(1_000_000_000),
        "MICROSECONDS" => Some(1_000),
        "NANOSECONDS" => Some(1),
        _ => None,
    }
}

const MEASURE_VALUE_TYPES: &[&str] = &[
    "DOUBLE",
    "BIGINT",
    "VARCHAR",
    "BOOLEAN",
    "TIMESTAMP",
    "MULTI",
];

impl TimestreamService {
    fn write_records(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let database = required_str(body, "DatabaseName")?;
        let table = required_str(body, "TableName")?;
        let key = table_key(&database, &table);
        let common = body.get("CommonAttributes");
        let records = body
            .get("Records")
            .and_then(Value::as_array)
            .ok_or_else(|| validation_exception("Records is required."))?;

        // Build (or reject) every record before touching state.
        let mut prepared: Vec<StoredRecord> = Vec::with_capacity(records.len());
        let mut rejected: Vec<Value> = Vec::new();
        for (idx, rec) in records.iter().enumerate() {
            match prepare_record(rec, common) {
                Ok(sr) => prepared.push(sr),
                Err(reason) => rejected.push(json!({
                    "RecordIndex": idx,
                    "Reason": reason,
                })),
            }
        }

        if !rejected.is_empty() {
            let rejected_json = serde_json::to_string(&rejected).unwrap_or_else(|_| "[]".into());
            return Err(AwsServiceError::aws_error_with_fields(
                StatusCode::BAD_REQUEST,
                "RejectedRecordsException",
                "One or more records have been rejected. See RejectedRecords for details.",
                vec![("RejectedRecords".to_string(), rejected_json)],
            ));
        }

        let total = prepared.len();
        {
            let mut guard = self.state.write();
            let data = guard.get_or_create(&req.account_id);
            if !data.tables.contains_key(&key) {
                return Err(resource_not_found(format!(
                    "Table '{table}' does not exist in database '{database}'."
                )));
            }
            let buf = data.records.entry(key).or_default();
            buf.extend(prepared);
            if buf.len() > MAX_RECORDS_PER_TABLE {
                let overflow = buf.len() - MAX_RECORDS_PER_TABLE;
                buf.drain(0..overflow);
            }
        }

        ok(json!({
            "RecordsIngested": {
                "Total": total,
                "MemoryStore": total,
                "MagneticStore": 0
            }
        }))
    }
}

/// Merge `CommonAttributes` under a record and validate it. Returns the
/// normalized [`StoredRecord`] or a human-readable rejection reason.
fn prepare_record(rec: &Value, common: Option<&Value>) -> Result<StoredRecord, String> {
    let get = |field: &str| -> Option<String> {
        rec.get(field)
            .and_then(Value::as_str)
            .or_else(|| common.and_then(|c| c.get(field)).and_then(Value::as_str))
            .map(str::to_string)
    };

    // Dimensions: common dimensions first, then record dimensions.
    let mut dimensions: Vec<(String, String)> = Vec::new();
    let mut push_dims = |v: Option<&Value>| -> Result<(), String> {
        if let Some(arr) = v.and_then(Value::as_array) {
            for d in arr {
                let name = d
                    .get("Name")
                    .and_then(Value::as_str)
                    .ok_or("A dimension is missing its Name.")?;
                let value = d
                    .get("Value")
                    .and_then(Value::as_str)
                    .ok_or("A dimension is missing its Value.")?;
                dimensions.push((name.to_string(), value.to_string()));
            }
        }
        Ok(())
    };
    push_dims(common.and_then(|c| c.get("Dimensions")))?;
    push_dims(rec.get("Dimensions"))?;

    let measure_value_type = get("MeasureValueType").unwrap_or_else(|| "DOUBLE".to_string());
    if !MEASURE_VALUE_TYPES.contains(&measure_value_type.as_str()) {
        return Err(format!(
            "MeasureValueType '{measure_value_type}' is not a valid measure value type."
        ));
    }

    let measure_name = get("MeasureName").unwrap_or_default();
    if measure_name.is_empty() {
        return Err("MeasureName is required.".to_string());
    }

    let measure_value = if measure_value_type == "MULTI" {
        let mv = rec
            .get("MeasureValues")
            .or_else(|| common.and_then(|c| c.get("MeasureValues")));
        match mv.and_then(Value::as_array) {
            Some(arr) if !arr.is_empty() => Value::Array(arr.clone()).to_string(),
            _ => {
                return Err("A MULTI measure record must carry non-empty MeasureValues.".to_string())
            }
        }
    } else {
        match get("MeasureValue") {
            Some(v) => v,
            None => return Err("MeasureValue is required for a scalar measure.".to_string()),
        }
    };

    let time_str = get("Time").ok_or("Time is required.")?;
    let time_unit = get("TimeUnit").unwrap_or_else(|| "MILLISECONDS".to_string());
    let mult = time_unit_multiplier(&time_unit)
        .ok_or_else(|| format!("TimeUnit '{time_unit}' is not a valid time unit."))?;
    let time_val: i128 = time_str
        .parse()
        .map_err(|_| format!("Time '{time_str}' is not a valid integer."))?;

    Ok(StoredRecord {
        time_nanos: time_val * mult,
        dimensions,
        measure_name,
        measure_value,
        measure_value_type,
    })
}

// ---- query ----------------------------------------------------------------

impl TimestreamService {
    fn query(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let query_string = required_str(body, "QueryString")?;
        let plan =
            query::parse_query(&query_string).map_err(|e| validation_exception(e.message()))?;
        let key = table_key(&plan.database, &plan.table);

        let result = {
            let guard = self.state.read();
            let data = guard.get(&req.account_id);
            let table_exists = data.map(|d| d.tables.contains_key(&key)).unwrap_or(false);
            if !table_exists {
                return Err(validation_exception(format!(
                    "The table \"{}\".\"{}\" does not exist.",
                    plan.database, plan.table
                )));
            }
            let empty: Vec<StoredRecord> = Vec::new();
            let records = data.and_then(|d| d.records.get(&key)).unwrap_or(&empty);
            query::execute(&plan, records)
        };

        // Paginate the row list with a round-tripping offset NextToken.
        let max_rows = body
            .get("MaxRows")
            .and_then(Value::as_u64)
            .map(|n| n.max(1) as usize);
        let start = decode_token(str_member(body, "NextToken"));
        let (rows, next) = match max_rows {
            Some(m) => paginate(&result.rows, start, m),
            None => (result.rows.clone(), None),
        };

        let mut out = Map::new();
        out.insert("QueryId".into(), json!(new_id()));
        out.insert("ColumnInfo".into(), Value::Array(result.columns));
        out.insert("Rows".into(), Value::Array(rows));
        out.insert(
            "QueryStatus".into(),
            json!({
                "ProgressPercentage": 100.0,
                "CumulativeBytesScanned": 0,
                "CumulativeBytesMetered": 0
            }),
        );
        if let Some(n) = next {
            out.insert("NextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }

    fn cancel_query(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let query_id = required_str(body, "QueryId")?;
        self.with_account_mut(req, |data| {
            data.active_queries.remove(&query_id);
        });
        ok(json!({ "CancellationMessage": "Query cancelled successfully." }))
    }

    fn prepare_query(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let query_string = required_str(body, "QueryString")?;
        let plan =
            query::parse_query(&query_string).map_err(|e| validation_exception(e.message()))?;
        let key = table_key(&plan.database, &plan.table);

        let columns = {
            let guard = self.state.read();
            let data = guard.get(&req.account_id);
            let empty: Vec<StoredRecord> = Vec::new();
            let records = data.and_then(|d| d.records.get(&key)).unwrap_or(&empty);
            let result = query::execute(&plan, records);
            if result.columns.is_empty() {
                vec![
                    json!({ "Name": "measure_name", "Type": { "ScalarType": "VARCHAR" } }),
                    json!({ "Name": "time", "Type": { "ScalarType": "TIMESTAMP" } }),
                ]
            } else {
                result.columns
            }
        };

        // Reshape ColumnInfo into SelectColumn (adds Database/Table/Aliased).
        let select_columns: Vec<Value> = columns
            .into_iter()
            .map(|c| {
                json!({
                    "Name": c.get("Name").cloned().unwrap_or(Value::Null),
                    "Type": c.get("Type").cloned().unwrap_or(Value::Null),
                    "DatabaseName": plan.database,
                    "TableName": plan.table,
                    "Aliased": false
                })
            })
            .collect();

        // Named parameters (`@name`) become ParameterMapping entries.
        let mut params: Vec<Value> = Vec::new();
        let mut seen: Vec<String> = Vec::new();
        for cap in regex_param().find_iter(&query_string) {
            let name = cap.as_str().to_string();
            if !seen.contains(&name) {
                seen.push(name.clone());
                params.push(json!({ "Name": name, "Type": { "ScalarType": "UNKNOWN" } }));
            }
        }

        ok(json!({
            "QueryString": query_string,
            "Columns": select_columns,
            "Parameters": params
        }))
    }
}

fn regex_param() -> &'static regex::Regex {
    use std::sync::OnceLock;
    static R: OnceLock<regex::Regex> = OnceLock::new();
    R.get_or_init(|| regex::Regex::new(r"@[A-Za-z_][A-Za-z0-9_]*").unwrap())
}

// ---- scheduled queries ----------------------------------------------------

impl ScheduledQuery {
    fn to_summary(&self) -> Value {
        let mut o = Map::new();
        o.insert("Arn".into(), json!(self.arn));
        o.insert("Name".into(), json!(self.name));
        o.insert("State".into(), json!(self.state));
        o.insert("CreationTime".into(), json!(self.creation_time));
        if let Some(t) = self.previous_invocation_time {
            o.insert("PreviousInvocationTime".into(), json!(t));
        }
        if let Some(e) = &self.error_report_configuration {
            o.insert("ErrorReportConfiguration".into(), e.clone());
        }
        Value::Object(o)
    }

    fn to_description(&self) -> Value {
        let mut o = Map::new();
        o.insert("Arn".into(), json!(self.arn));
        o.insert("Name".into(), json!(self.name));
        o.insert("QueryString".into(), json!(self.query_string));
        o.insert("State".into(), json!(self.state));
        o.insert("CreationTime".into(), json!(self.creation_time));
        o.insert(
            "ScheduleConfiguration".into(),
            self.schedule_configuration.clone(),
        );
        o.insert(
            "NotificationConfiguration".into(),
            self.notification_configuration.clone(),
        );
        if let Some(t) = &self.target_configuration {
            o.insert("TargetConfiguration".into(), t.clone());
        }
        if let Some(r) = &self.scheduled_query_execution_role_arn {
            o.insert("ScheduledQueryExecutionRoleArn".into(), json!(r));
        }
        if let Some(k) = &self.kms_key_id {
            o.insert("KmsKeyId".into(), json!(k));
        }
        if let Some(e) = &self.error_report_configuration {
            o.insert("ErrorReportConfiguration".into(), e.clone());
        }
        if let Some(t) = self.previous_invocation_time {
            o.insert("PreviousInvocationTime".into(), json!(t));
        }
        Value::Object(o)
    }
}

impl TimestreamService {
    fn create_scheduled_query(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_str(body, "Name")?;
        let query_string = required_str(body, "QueryString")?;
        let region = req.region.clone();
        let account = req.account_id.clone();
        let schedule = body
            .get("ScheduleConfiguration")
            .cloned()
            .ok_or_else(|| validation_exception("ScheduleConfiguration is required."))?;
        let notification = body
            .get("NotificationConfiguration")
            .cloned()
            .ok_or_else(|| validation_exception("NotificationConfiguration is required."))?;
        let error_report = body.get("ErrorReportConfiguration").cloned();
        let target = body.get("TargetConfiguration").cloned();
        let role = str_member(body, "ScheduledQueryExecutionRoleArn").map(str::to_string);
        let kms = str_member(body, "KmsKeyId").map(str::to_string);
        let tags = body.get("Tags").cloned();
        let arn = scheduled_query_arn(&region, &account, &name, &new_id());
        self.with_account_mut(req, |data| {
            let sq = ScheduledQuery {
                arn: arn.clone(),
                name,
                query_string,
                state: "ENABLED".to_string(),
                creation_time: now_epoch(),
                schedule_configuration: schedule,
                notification_configuration: notification,
                target_configuration: target,
                scheduled_query_execution_role_arn: role,
                kms_key_id: kms,
                error_report_configuration: error_report,
                previous_invocation_time: None,
                last_run_status: None,
            };
            data.scheduled_queries.insert(arn.clone(), sq);
            let entry = data.tags.entry(arn.clone()).or_default();
            absorb_tags(tags.as_ref(), entry);
            ok(json!({ "Arn": arn }))
        })
    }

    fn describe_scheduled_query(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_str(body, "ScheduledQueryArn")?;
        let guard = self.state.read();
        let sq = guard
            .get(&req.account_id)
            .and_then(|d| d.scheduled_queries.get(&arn))
            .ok_or_else(|| {
                resource_not_found(format!("Scheduled query '{arn}' does not exist."))
            })?;
        ok(json!({ "ScheduledQuery": sq.to_description() }))
    }

    fn list_scheduled_queries(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let max = body
            .get("MaxResults")
            .and_then(Value::as_u64)
            .unwrap_or(100)
            .max(1) as usize;
        let start = decode_token(str_member(body, "NextToken"));
        let guard = self.state.read();
        let all: Vec<Value> = guard
            .get(&req.account_id)
            .map(|d| {
                d.scheduled_queries
                    .values()
                    .map(ScheduledQuery::to_summary)
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(&all, start, max);
        let mut out = Map::new();
        out.insert("ScheduledQueries".into(), Value::Array(page));
        if let Some(n) = next {
            out.insert("NextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }

    fn update_scheduled_query(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_str(body, "ScheduledQueryArn")?;
        let state = required_str(body, "State")?;
        self.with_account_mut(req, |data| {
            let sq = data.scheduled_queries.get_mut(&arn).ok_or_else(|| {
                resource_not_found(format!("Scheduled query '{arn}' does not exist."))
            })?;
            sq.state = state;
            empty_ok()
        })
    }

    fn delete_scheduled_query(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_str(body, "ScheduledQueryArn")?;
        self.with_account_mut(req, |data| {
            if data.scheduled_queries.remove(&arn).is_none() {
                return Err(resource_not_found(format!(
                    "Scheduled query '{arn}' does not exist."
                )));
            }
            data.tags.remove(&arn);
            empty_ok()
        })
    }

    fn execute_scheduled_query(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_str(body, "ScheduledQueryArn")?;
        let invocation = body.get("InvocationTime").and_then(Value::as_f64);
        self.with_account_mut(req, |data| {
            let sq = data.scheduled_queries.get_mut(&arn).ok_or_else(|| {
                resource_not_found(format!("Scheduled query '{arn}' does not exist."))
            })?;
            sq.previous_invocation_time = invocation.or_else(|| Some(now_epoch()));
            sq.last_run_status = Some("MANUAL_TRIGGER_SUCCESS".to_string());
            empty_ok()
        })
    }
}

// ---- batch load -----------------------------------------------------------

impl BatchLoadTask {
    fn to_summary(&self) -> Value {
        json!({
            "TaskId": self.task_id,
            "TaskStatus": self.status,
            "DatabaseName": self.target_database_name,
            "TableName": self.target_table_name,
            "CreationTime": self.creation_time,
            "LastUpdatedTime": self.last_updated_time,
            "ResumableUntil": self.resumable_until
        })
    }

    fn to_description(&self) -> Value {
        let mut o = Map::new();
        o.insert("TaskId".into(), json!(self.task_id));
        o.insert("TaskStatus".into(), json!(self.status));
        o.insert(
            "TargetDatabaseName".into(),
            json!(self.target_database_name),
        );
        o.insert("TargetTableName".into(), json!(self.target_table_name));
        o.insert(
            "DataSourceConfiguration".into(),
            self.data_source_configuration.clone(),
        );
        o.insert(
            "ReportConfiguration".into(),
            self.report_configuration.clone(),
        );
        if let Some(d) = &self.data_model_configuration {
            o.insert("DataModelConfiguration".into(), d.clone());
        }
        if let Some(v) = self.record_version {
            o.insert("RecordVersion".into(), json!(v));
        }
        o.insert("CreationTime".into(), json!(self.creation_time));
        o.insert("LastUpdatedTime".into(), json!(self.last_updated_time));
        o.insert("ResumableUntil".into(), json!(self.resumable_until));
        Value::Object(o)
    }
}

impl TimestreamService {
    fn create_batch_load_task(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let target_db = required_str(body, "TargetDatabaseName")?;
        let target_table = required_str(body, "TargetTableName")?;
        let data_source = body
            .get("DataSourceConfiguration")
            .cloned()
            .ok_or_else(|| validation_exception("DataSourceConfiguration is required."))?;
        let report = body
            .get("ReportConfiguration")
            .cloned()
            .ok_or_else(|| validation_exception("ReportConfiguration is required."))?;
        let data_model = body.get("DataModelConfiguration").cloned();
        let record_version = body.get("RecordVersion").and_then(Value::as_i64);
        let task_id = new_id();
        self.with_account_mut(req, |data| {
            let now = now_epoch();
            let task = BatchLoadTask {
                task_id: task_id.clone(),
                status: "CREATED".to_string(),
                target_database_name: target_db,
                target_table_name: target_table,
                data_source_configuration: data_source,
                report_configuration: report,
                data_model_configuration: data_model,
                record_version,
                creation_time: now,
                last_updated_time: now,
                resumable_until: now + 86_400.0,
            };
            data.batch_load_tasks.insert(task_id.clone(), task);
            ok(json!({ "TaskId": task_id }))
        })
    }

    fn describe_batch_load_task(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let task_id = required_str(body, "TaskId")?;
        let guard = self.state.read();
        let task = guard
            .get(&req.account_id)
            .and_then(|d| d.batch_load_tasks.get(&task_id))
            .ok_or_else(|| {
                resource_not_found(format!("Batch load task '{task_id}' does not exist."))
            })?;
        ok(json!({ "BatchLoadTaskDescription": task.to_description() }))
    }

    fn list_batch_load_tasks(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let status_filter = str_member(body, "TaskStatus").map(str::to_string);
        let max = body
            .get("MaxResults")
            .and_then(Value::as_u64)
            .unwrap_or(100)
            .max(1) as usize;
        let start = decode_token(str_member(body, "NextToken"));
        let guard = self.state.read();
        let all: Vec<Value> = guard
            .get(&req.account_id)
            .map(|d| {
                d.batch_load_tasks
                    .values()
                    .filter(|t| {
                        status_filter
                            .as_ref()
                            .map(|s| &t.status == s)
                            .unwrap_or(true)
                    })
                    .map(BatchLoadTask::to_summary)
                    .collect()
            })
            .unwrap_or_default();
        let (page, next) = paginate(&all, start, max);
        let mut out = Map::new();
        out.insert("BatchLoadTasks".into(), Value::Array(page));
        if let Some(n) = next {
            out.insert("NextToken".into(), json!(n));
        }
        ok(Value::Object(out))
    }

    fn resume_batch_load_task(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let task_id = required_str(body, "TaskId")?;
        self.with_account_mut(req, |data| {
            let task = data.batch_load_tasks.get_mut(&task_id).ok_or_else(|| {
                resource_not_found(format!("Batch load task '{task_id}' does not exist."))
            })?;
            task.status = "IN_PROGRESS".to_string();
            task.last_updated_time = now_epoch();
            empty_ok()
        })
    }
}

// ---- account settings -----------------------------------------------------

/// Map an `UpdateAccountSettings` request `QueryComputeRequest` onto the
/// `QueryComputeResponse` shape the API returns: `ProvisionedCapacity`'s
/// request-only `TargetQueryTCU` becomes the response's `ActiveQueryTCU`, and a
/// settled `LastUpdate` is synthesized. `ComputeMode` and
/// `NotificationConfiguration` pass through.
fn query_compute_to_response(req: &Value) -> Value {
    let mut out = Map::new();
    if let Some(mode) = req.get("ComputeMode") {
        out.insert("ComputeMode".into(), mode.clone());
    }
    if let Some(pc) = req.get("ProvisionedCapacity") {
        let target = pc.get("TargetQueryTCU").and_then(Value::as_i64);
        let mut pc_out = Map::new();
        if let Some(t) = target {
            pc_out.insert("ActiveQueryTCU".into(), json!(t));
            pc_out.insert(
                "LastUpdate".into(),
                json!({ "TargetQueryTCU": t, "Status": "SUCCEEDED" }),
            );
        }
        if let Some(n) = pc.get("NotificationConfiguration") {
            pc_out.insert("NotificationConfiguration".into(), n.clone());
        }
        out.insert("ProvisionedCapacity".into(), Value::Object(pc_out));
    }
    Value::Object(out)
}

impl AccountSettings {
    fn to_wire(&self) -> Value {
        let mut o = Map::new();
        if let Some(t) = self.max_query_tcu {
            o.insert("MaxQueryTCU".into(), json!(t));
        }
        o.insert("QueryPricingModel".into(), json!(self.query_pricing_model));
        if let Some(c) = &self.query_compute {
            o.insert("QueryCompute".into(), c.clone());
        }
        Value::Object(o)
    }
}

impl TimestreamService {
    fn describe_account_settings(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let settings = guard
            .get(&req.account_id)
            .map(|d| d.account_settings.clone())
            .unwrap_or_default();
        ok(settings.to_wire())
    }

    fn update_account_settings(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let max_tcu = body.get("MaxQueryTCU").and_then(Value::as_i64);
        let pricing = str_member(body, "QueryPricingModel").map(str::to_string);
        // The request carries a `QueryComputeRequest`
        // (`ProvisionedCapacity.TargetQueryTCU`); the response expects a
        // `QueryComputeResponse` (`ProvisionedCapacity.ActiveQueryTCU` + a
        // `LastUpdate`). Map it so a `Describe` / `Update` echo is shape-valid.
        let compute = body.get("QueryCompute").map(query_compute_to_response);
        self.with_account_mut(req, |data| {
            if let Some(t) = max_tcu {
                data.account_settings.max_query_tcu = Some(t);
            }
            if let Some(p) = pricing {
                data.account_settings.query_pricing_model = p;
            }
            if compute.is_some() {
                data.account_settings.query_compute = compute;
            }
            ok(data.account_settings.to_wire())
        })
    }
}

// ---- endpoints + tagging --------------------------------------------------

impl TimestreamService {
    fn describe_endpoints(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        ok(json!({
            "Endpoints": [
                {
                    "Address": endpoint_address(req),
                    "CachePeriodInMinutes": 1440
                }
            ]
        }))
    }

    fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_str(body, "ResourceARN")?;
        let guard = self.state.read();
        let tags = guard
            .get(&req.account_id)
            .and_then(|d| d.tags.get(&arn))
            .cloned()
            .unwrap_or_default();
        ok(json!({ "Tags": tag_list(&tags) }))
    }

    fn tag_resource(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_str(body, "ResourceARN")?;
        let tags = body.get("Tags").cloned();
        self.with_account_mut(req, |data| {
            let entry = data.tags.entry(arn).or_default();
            absorb_tags(tags.as_ref(), entry);
        });
        empty_ok()
    }

    fn untag_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_str(body, "ResourceARN")?;
        let keys: Vec<String> = body
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        self.with_account_mut(req, |data| {
            if let Some(entry) = data.tags.get_mut(&arn) {
                for k in &keys {
                    entry.remove(k);
                }
            }
        });
        empty_ok()
    }
}

// ---- pagination -----------------------------------------------------------

/// Decode a numeric offset NextToken; a missing/garbage token starts at 0.
fn decode_token(token: Option<&str>) -> usize {
    token.and_then(|t| t.parse::<usize>().ok()).unwrap_or(0)
}

/// Slice `items[start..]` to at most `max` entries, returning the page plus the
/// next offset token when more remain.
fn paginate(items: &[Value], start: usize, max: usize) -> (Vec<Value>, Option<String>) {
    if start >= items.len() {
        return (Vec::new(), None);
    }
    let end = (start + max).min(items.len());
    let page = items[start..end].to_vec();
    let next = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> TimestreamService {
        TimestreamService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "http://localhost:4566",
        ))))
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "timestream".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: String::new(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn call(s: &TimestreamService, action: &str, body: Value) -> Result<Value, AwsServiceError> {
        let r = req(action, body);
        let resp = s.dispatch(action, &r)?;
        Ok(serde_json::from_slice(resp.body.expect_bytes()).unwrap())
    }

    #[test]
    fn database_lifecycle() {
        let s = svc();
        let out = call(&s, "CreateDatabase", json!({ "DatabaseName": "metrics" })).unwrap();
        assert_eq!(out["Database"]["DatabaseName"], "metrics");
        assert!(out["Database"]["Arn"]
            .as_str()
            .unwrap()
            .ends_with(":database/metrics"));

        // Duplicate is a conflict.
        let e = call(&s, "CreateDatabase", json!({ "DatabaseName": "metrics" })).unwrap_err();
        assert_eq!(e.code(), "ConflictException");

        let d = call(&s, "DescribeDatabase", json!({ "DatabaseName": "metrics" })).unwrap();
        assert_eq!(d["Database"]["TableCount"], 0);

        let l = call(&s, "ListDatabases", json!({})).unwrap();
        assert_eq!(l["Databases"].as_array().unwrap().len(), 1);

        call(&s, "DeleteDatabase", json!({ "DatabaseName": "metrics" })).unwrap();
        let e = call(&s, "DescribeDatabase", json!({ "DatabaseName": "metrics" })).unwrap_err();
        assert_eq!(e.code(), "ResourceNotFoundException");
    }

    #[test]
    fn table_requires_database() {
        let s = svc();
        let e = call(
            &s,
            "CreateTable",
            json!({ "DatabaseName": "nope", "TableName": "t" }),
        )
        .unwrap_err();
        assert_eq!(e.code(), "ResourceNotFoundException");
    }

    #[test]
    fn delete_non_empty_database_is_rejected() {
        let s = svc();
        call(&s, "CreateDatabase", json!({ "DatabaseName": "m" })).unwrap();
        call(
            &s,
            "CreateTable",
            json!({ "DatabaseName": "m", "TableName": "t" }),
        )
        .unwrap();
        let e = call(&s, "DeleteDatabase", json!({ "DatabaseName": "m" })).unwrap_err();
        assert_eq!(e.code(), "ValidationException");
    }

    #[test]
    fn write_and_query_round_trip() {
        let s = svc();
        call(&s, "CreateDatabase", json!({ "DatabaseName": "m" })).unwrap();
        call(
            &s,
            "CreateTable",
            json!({ "DatabaseName": "m", "TableName": "t" }),
        )
        .unwrap();
        let w = call(
            &s,
            "WriteRecords",
            json!({
                "DatabaseName": "m",
                "TableName": "t",
                "Records": [
                    {
                        "Dimensions": [ { "Name": "host", "Value": "a" } ],
                        "MeasureName": "cpu",
                        "MeasureValue": "1.5",
                        "MeasureValueType": "DOUBLE",
                        "Time": "1000",
                        "TimeUnit": "MILLISECONDS"
                    }
                ]
            }),
        )
        .unwrap();
        assert_eq!(w["RecordsIngested"]["Total"], 1);

        let q = call(
            &s,
            "Query",
            json!({ "QueryString": "SELECT * FROM \"m\".\"t\"" }),
        )
        .unwrap();
        assert_eq!(q["Rows"].as_array().unwrap().len(), 1);
        assert!(q["QueryId"].as_str().is_some());

        let c = call(
            &s,
            "Query",
            json!({ "QueryString": "SELECT COUNT(*) FROM \"m\".\"t\"" }),
        )
        .unwrap();
        assert_eq!(c["Rows"][0]["Data"][0]["ScalarValue"], "1");
    }

    #[test]
    fn write_records_rejects_bad_record() {
        let s = svc();
        call(&s, "CreateDatabase", json!({ "DatabaseName": "m" })).unwrap();
        call(
            &s,
            "CreateTable",
            json!({ "DatabaseName": "m", "TableName": "t" }),
        )
        .unwrap();
        let e = call(
            &s,
            "WriteRecords",
            json!({
                "DatabaseName": "m",
                "TableName": "t",
                "Records": [ { "MeasureValue": "1", "Time": "1" } ]
            }),
        )
        .unwrap_err();
        assert_eq!(e.code(), "RejectedRecordsException");
    }

    #[test]
    fn unsupported_query_is_validation() {
        let s = svc();
        call(&s, "CreateDatabase", json!({ "DatabaseName": "m" })).unwrap();
        call(
            &s,
            "CreateTable",
            json!({ "DatabaseName": "m", "TableName": "t" }),
        )
        .unwrap();
        let e = call(
            &s,
            "Query",
            json!({ "QueryString": "SELECT avg(x) FROM \"m\".\"t\"" }),
        )
        .unwrap_err();
        assert_eq!(e.code(), "ValidationException");
    }

    #[test]
    fn scheduled_query_crud() {
        let s = svc();
        let out = call(
            &s,
            "CreateScheduledQuery",
            json!({
                "Name": "sq",
                "QueryString": "SELECT * FROM \"m\".\"t\"",
                "ScheduleConfiguration": { "ScheduleExpression": "rate(1 hour)" },
                "NotificationConfiguration": { "SnsConfiguration": { "TopicArn": "arn:aws:sns:us-east-1:000000000000:t" } },
                "ScheduledQueryExecutionRoleArn": "arn:aws:iam::000000000000:role/r",
                "ErrorReportConfiguration": { "S3Configuration": { "BucketName": "b" } }
            }),
        )
        .unwrap();
        let arn = out["Arn"].as_str().unwrap().to_string();
        let d = call(
            &s,
            "DescribeScheduledQuery",
            json!({ "ScheduledQueryArn": arn }),
        )
        .unwrap();
        assert_eq!(d["ScheduledQuery"]["State"], "ENABLED");
        call(
            &s,
            "UpdateScheduledQuery",
            json!({ "ScheduledQueryArn": arn, "State": "DISABLED" }),
        )
        .unwrap();
        let d = call(
            &s,
            "DescribeScheduledQuery",
            json!({ "ScheduledQueryArn": arn }),
        )
        .unwrap();
        assert_eq!(d["ScheduledQuery"]["State"], "DISABLED");
        call(
            &s,
            "DeleteScheduledQuery",
            json!({ "ScheduledQueryArn": arn }),
        )
        .unwrap();
    }

    #[test]
    fn describe_endpoints_echoes_host() {
        let s = svc();
        let mut r = req("DescribeEndpoints", json!({}));
        r.headers.insert("host", "localhost:4566".parse().unwrap());
        let resp = s.dispatch("DescribeEndpoints", &r).unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["Endpoints"][0]["Address"], "localhost:4566");
        assert_eq!(v["Endpoints"][0]["CachePeriodInMinutes"], 1440);
    }

    #[test]
    fn account_settings_round_trip() {
        let s = svc();
        let d = call(&s, "DescribeAccountSettings", json!({})).unwrap();
        assert_eq!(d["QueryPricingModel"], "BYTES_SCANNED");
        let u = call(
            &s,
            "UpdateAccountSettings",
            json!({ "MaxQueryTCU": 4, "QueryPricingModel": "COMPUTE_UNITS" }),
        )
        .unwrap();
        assert_eq!(u["MaxQueryTCU"], 4);
        assert_eq!(u["QueryPricingModel"], "COMPUTE_UNITS");
    }

    #[test]
    fn tagging_round_trip() {
        let s = svc();
        let out = call(&s, "CreateDatabase", json!({ "DatabaseName": "m" })).unwrap();
        let arn = out["Database"]["Arn"].as_str().unwrap().to_string();
        call(
            &s,
            "TagResource",
            json!({ "ResourceARN": arn, "Tags": [ { "Key": "env", "Value": "prod" } ] }),
        )
        .unwrap();
        let t = call(&s, "ListTagsForResource", json!({ "ResourceARN": arn })).unwrap();
        assert_eq!(t["Tags"][0]["Key"], "env");
        call(
            &s,
            "UntagResource",
            json!({ "ResourceARN": arn, "TagKeys": ["env"] }),
        )
        .unwrap();
        let t = call(&s, "ListTagsForResource", json!({ "ResourceARN": arn })).unwrap();
        assert_eq!(t["Tags"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn batch_load_lifecycle() {
        let s = svc();
        let out = call(
            &s,
            "CreateBatchLoadTask",
            json!({
                "TargetDatabaseName": "m",
                "TargetTableName": "t",
                "DataSourceConfiguration": { "DataSourceS3Configuration": { "BucketName": "b" }, "DataFormat": "CSV" },
                "ReportConfiguration": { "ReportS3Configuration": { "BucketName": "r" } }
            }),
        )
        .unwrap();
        let task_id = out["TaskId"].as_str().unwrap().to_string();
        let d = call(&s, "DescribeBatchLoadTask", json!({ "TaskId": task_id })).unwrap();
        assert_eq!(d["BatchLoadTaskDescription"]["TaskStatus"], "CREATED");
        let l = call(&s, "ListBatchLoadTasks", json!({})).unwrap();
        assert_eq!(l["BatchLoadTasks"].as_array().unwrap().len(), 1);
    }
}

#[cfg(test)]
mod async_handle_tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use serde_json::json;

    fn areq(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "timestream".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: String::new(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[tokio::test]
    async fn handle_describe_endpoints_completes() {
        let s = TimestreamService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "http://localhost:4566",
        ))));
        let resp = s
            .handle(areq("DescribeEndpoints", json!({})))
            .await
            .unwrap();
        assert!(resp.status.is_success());
    }

    #[tokio::test]
    async fn handle_create_database_completes() {
        let s = TimestreamService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "http://localhost:4566",
        ))));
        let resp = s
            .handle(areq("CreateDatabase", json!({ "DatabaseName": "m" })))
            .await
            .unwrap();
        assert!(resp.status.is_success());
    }
}
