//! RDS Data API (`rds-data`) restJson1 dispatch + real SQL execution.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Map, Value};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_rds::SharedRdsState;

const SUPPORTED_ACTIONS: &[&str] = &[
    "ExecuteStatement",
    "BatchExecuteStatement",
    "BeginTransaction",
    "CommitTransaction",
    "RollbackTransaction",
    "ExecuteSql",
];

/// Idle transactions are rolled back and their connection released after this
/// long with no statement, mirroring the AWS RDS Data API's ~3-minute idle
/// transaction timeout. Without this, a client that begins a transaction and
/// never commits (crash, dropped handle) would leak a backing DB connection
/// forever, eventually exhausting the container's `max_connections`.
const TXN_IDLE_TIMEOUT: Duration = Duration::from_secs(180);

/// Connection parameters captured from a resolved `DbInstance` (owned so the
/// state read-lock is dropped before we touch the network).
struct DbConn {
    engine: String,
    host: String,
    port: u16,
    user: String,
    password: String,
    db_name: String,
    /// Optional schema (`search_path` on Postgres); ignored on MySQL where the
    /// schema is the database.
    schema: Option<String>,
}

/// A live connection held open across requests for the lifetime of a
/// transaction. Postgres drives its connection on a spawned task that lives as
/// long as the `Client`; MySQL owns its socket.
enum HeldConn {
    Pg(tokio_postgres::Client),
    MySql(mysql_async::Conn),
}

/// One open transaction. The connection has its own lock so the global
/// transactions-map lock is never held across an awaited SQL statement (a slow
/// statement in one transaction must not block begin/commit/rollback of
/// another). `last_used` drives the idle reaper.
struct TxnEntry {
    /// `None` once the transaction has been committed/rolled-back or reaped.
    conn: Mutex<Option<HeldConn>>,
    last_used: StdMutex<Instant>,
    /// Number of statements currently executing against this transaction. Bumped
    /// while the transactions-map lock is held (so the reaper, which samples
    /// under the same lock, can never see a live in-flight transaction as
    /// reapable) and cleared when the statement finishes. A transaction is only
    /// reapable when this is zero.
    in_flight: AtomicU64,
}

impl TxnEntry {
    fn touch(&self) {
        if let Ok(mut t) = self.last_used.lock() {
            *t = Instant::now();
        }
    }

    /// Has this transaction been idle longer than [`TXN_IDLE_TIMEOUT`] as of
    /// `now`? A poisoned `last_used` lock is treated as not-stale so a spurious
    /// panic elsewhere can never trigger a rollback.
    fn is_stale(&self, now: Instant) -> bool {
        self.last_used
            .lock()
            .map(|t| now.duration_since(*t) > TXN_IDLE_TIMEOUT)
            .unwrap_or(false)
    }

    /// May the reaper roll this transaction back? Only when no statement is
    /// in-flight *and* it has been idle past the timeout. The `in_flight` check
    /// closes the window where a statement has taken a reference to the entry but
    /// has not yet refreshed `last_used`.
    fn is_reapable(&self, now: Instant) -> bool {
        self.in_flight.load(Ordering::SeqCst) == 0 && self.is_stale(now)
    }
}

/// RAII marker that keeps a transaction pinned against the idle reaper for the
/// duration of a statement. Incremented under the transactions-map lock in
/// [`RdsDataService::acquire_txn`]; on drop it refreshes the idle clock and then
/// releases the pin, so the moment the pin is gone `last_used` is already fresh.
struct InFlightGuard {
    entry: Arc<TxnEntry>,
}

impl InFlightGuard {
    fn pin(entry: Arc<TxnEntry>) -> Self {
        entry.in_flight.fetch_add(1, Ordering::SeqCst);
        entry.touch();
        Self { entry }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        // Refresh the idle clock *before* dropping the pin: once `in_flight`
        // reaches zero the reaper may observe the entry, and it must then see an
        // up-to-date `last_used` rather than the stale pre-statement value.
        self.entry.touch();
        self.entry.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

type TxnMap = Arc<Mutex<HashMap<String, Arc<TxnEntry>>>>;

pub struct RdsDataService {
    rds_state: SharedRdsState,
    /// Open transactions: `transactionId` -> the held connection running its
    /// `BEGIN`.
    transactions: TxnMap,
}

impl RdsDataService {
    pub fn new(rds_state: SharedRdsState) -> Self {
        let transactions: TxnMap = Arc::new(Mutex::new(HashMap::new()));
        // Reap idle transactions so abandoned ones don't leak DB connections.
        // Only spawn when a tokio runtime is present (it always is in the
        // server; absent in some unit-test constructions).
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(reap_idle_transactions(transactions.clone()));
        }
        Self {
            rds_state,
            transactions,
        }
    }

    /// Map the restJson1 request (`POST /<Op>`) to its operation name.
    fn resolve_action(req: &AwsRequest) -> Option<&'static str> {
        if req.method != Method::POST {
            return None;
        }
        match req.path_segments.first().map(String::as_str)? {
            "Execute" => Some("ExecuteStatement"),
            "BatchExecute" => Some("BatchExecuteStatement"),
            "BeginTransaction" => Some("BeginTransaction"),
            "CommitTransaction" => Some("CommitTransaction"),
            "RollbackTransaction" => Some("RollbackTransaction"),
            "ExecuteSql" => Some("ExecuteSql"),
            _ => None,
        }
    }

    /// Resolve a `resourceArn` (a DB instance or Aurora cluster ARN) to live
    /// connection parameters from the RDS service state. Returns the writer
    /// instance for a cluster ARN.
    fn resolve_conn(&self, account_id: &str, resource_arn: &str) -> Option<DbConn> {
        let cluster_id = resource_arn
            .rsplit_once(":cluster:")
            .map(|(_, id)| id.to_string());
        let accounts = self.rds_state.read();
        let state = accounts.get(account_id)?;
        let inst = state.instances.values().find(|i| {
            i.db_instance_arn == resource_arn
                || cluster_id
                    .as_deref()
                    .is_some_and(|c| i.db_cluster_identifier.as_deref() == Some(c))
        })?;
        Some(DbConn {
            engine: inst.engine.clone(),
            // Use the address the RDS runtime actually recorded for this
            // instance (Docker sibling host or k8s Pod IP). Re-deriving a
            // Docker-only sibling host here would break the k8s backend and
            // shell out to `docker info` on every request.
            host: inst.endpoint_address.clone(),
            port: inst.host_port,
            user: inst.master_username.clone(),
            password: inst.master_user_password.clone(),
            db_name: inst.db_name.clone().unwrap_or_default(),
            schema: None,
        })
    }

    /// Common front-matter for the credentialed ops: validate `secretArn`,
    /// resolve the `resourceArn` to a connection, and apply the request's
    /// `database`/`schema` overrides (AWS lets a single resource serve many
    /// databases — Aurora clusters often have no default DB name at all).
    fn require_conn(&self, req: &AwsRequest, body: &Value) -> Result<DbConn, AwsServiceError> {
        let resource_arn = body
            .get("resourceArn")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("resourceArn is required"))?;
        // secretArn is required by AWS but fakecloud trusts the resourceArn for
        // credential resolution (the secret would carry the same master creds).
        if body.get("secretArn").and_then(Value::as_str).is_none() {
            return Err(bad_request("secretArn is required"));
        }
        let mut conn = self
            .resolve_conn(&req.account_id, resource_arn)
            .ok_or_else(|| {
                bad_request(format!(
                    "HttpEndpoint is not enabled for resource {resource_arn} (no such DB)"
                ))
            })?;
        if let Some(db) = body.get("database").and_then(Value::as_str) {
            if !db.is_empty() {
                conn.db_name = db.to_string();
            }
        }
        conn.schema = body
            .get("schema")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        Ok(conn)
    }

    async fn execute_statement(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let sql = body
            .get("sql")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("sql is required"))?;
        let include_metadata = body
            .get("includeResultMetadata")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let format_json = body
            .get("formatRecordsAs")
            .and_then(Value::as_str)
            .map(|f| f == "JSON")
            .unwrap_or(false);
        let params = parse_parameters(body.get("parameters"));

        // Inside a transaction: run on the held connection so the statement
        // shares the open transaction's visibility and is committed/rolled back
        // atomically with the others.
        if let Some(txid) = body.get("transactionId").and_then(Value::as_str) {
            // Validate the resource still resolves (AWS still checks it), but the
            // actual execution rides the held connection.
            self.require_conn(req, &body)?;
            let pin = self.acquire_txn(txid).await?;
            // Lock only this transaction's connection, not the whole map, so a
            // slow statement here can't block other transactions. The pin keeps
            // the reaper off this transaction for the whole statement.
            let mut guard = pin.entry.conn.lock().await;
            let held = guard.as_mut().ok_or_else(|| not_found_txn(txid))?;
            let value = match held {
                HeldConn::Pg(client) => {
                    pg_run(client, sql, &params, include_metadata, format_json).await?
                }
                HeldConn::MySql(conn) => {
                    my_run(conn, sql, &params, include_metadata, format_json).await?
                }
            };
            return Ok(AwsResponse::ok_json(value));
        }

        let conn = self.require_conn(req, &body)?;
        let engine = conn.engine.to_lowercase();
        let value = if engine.contains("postgres") {
            let client = pg_connect(&conn).await?;
            pg_run(&client, sql, &params, include_metadata, format_json).await?
        } else if is_mysql(&engine) {
            let mut c = my_connect(&conn).await?;
            let v = my_run(&mut c, sql, &params, include_metadata, format_json).await;
            let _ = c.disconnect().await;
            v?
        } else {
            return Err(unsupported_engine(&conn.engine));
        };
        Ok(AwsResponse::ok_json(value))
    }

    async fn begin_transaction(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let conn = self.require_conn(req, &body)?;
        let engine = conn.engine.to_lowercase();

        let held = if engine.contains("postgres") {
            let client = pg_connect(&conn).await?;
            client
                .batch_execute("BEGIN")
                .await
                .map_err(|e| bad_request(format!("could not begin transaction: {e}")))?;
            HeldConn::Pg(client)
        } else if is_mysql(&engine) {
            use mysql_async::prelude::Queryable;
            let mut c = my_connect(&conn).await?;
            c.query_drop("START TRANSACTION")
                .await
                .map_err(|e| bad_request(format!("could not begin transaction: {e}")))?;
            HeldConn::MySql(c)
        } else {
            return Err(unsupported_engine(&conn.engine));
        };

        let txid = gen_transaction_id();
        let entry = Arc::new(TxnEntry {
            conn: Mutex::new(Some(held)),
            last_used: StdMutex::new(Instant::now()),
            in_flight: AtomicU64::new(0),
        });
        self.transactions.lock().await.insert(txid.clone(), entry);
        Ok(AwsResponse::ok_json(json!({ "transactionId": txid })))
    }

    /// Look up an open transaction and pin it against the idle reaper for the
    /// duration of a statement. The `in_flight` bump happens while the
    /// transactions-map lock is still held, so the reaper (which samples under
    /// that same lock) can never observe this transaction as reapable between the
    /// lookup and the pin — closing the clone-then-touch race. The returned guard
    /// refreshes `last_used` and releases the pin on drop.
    async fn acquire_txn(&self, txid: &str) -> Result<InFlightGuard, AwsServiceError> {
        let map = self.transactions.lock().await;
        let entry = map.get(txid).cloned().ok_or_else(|| not_found_txn(txid))?;
        Ok(InFlightGuard::pin(entry))
    }

    /// Shared body for Commit/Rollback: pull the held connection out of the map
    /// and run the terminal SQL on it.
    async fn finish_transaction(
        &self,
        req: &AwsRequest,
        sql_keyword: &str,
        status: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let txid = body
            .get("transactionId")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("transactionId is required"))?;
        let entry = self
            .transactions
            .lock()
            .await
            .remove(txid)
            .ok_or_else(|| not_found_txn(txid))?;
        // Take ownership of the connection (waiting for any in-flight statement
        // on it to finish) and run the terminal SQL.
        let held = entry
            .conn
            .lock()
            .await
            .take()
            .ok_or_else(|| not_found_txn(txid))?;
        finish_held(held, sql_keyword).await?;
        Ok(AwsResponse::ok_json(json!({ "transactionStatus": status })))
    }

    async fn batch_execute_statement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let sql = body
            .get("sql")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("sql is required"))?;
        let sets = parse_parameter_sets(body.get("parameterSets"));

        // Run every parameter set on one connection so the whole batch shares a
        // transaction when `transactionId` is given.
        if let Some(txid) = body.get("transactionId").and_then(Value::as_str) {
            self.require_conn(req, &body)?;
            let pin = self.acquire_txn(txid).await?;
            let mut guard = pin.entry.conn.lock().await;
            let held = guard.as_mut().ok_or_else(|| not_found_txn(txid))?;
            let mut results = Vec::with_capacity(sets.len().max(1));
            match held {
                HeldConn::Pg(client) => {
                    for set in run_each(&sets) {
                        let v = pg_run(client, sql, set, false, false).await?;
                        results.push(batch_update_result(&v));
                    }
                }
                HeldConn::MySql(conn) => {
                    for set in run_each(&sets) {
                        let v = my_run(conn, sql, set, false, false).await?;
                        results.push(batch_update_result(&v));
                    }
                }
            }
            return Ok(AwsResponse::ok_json(json!({ "updateResults": results })));
        }

        let conn = self.require_conn(req, &body)?;
        let engine = conn.engine.to_lowercase();
        let mut results = Vec::with_capacity(sets.len().max(1));
        if engine.contains("postgres") {
            let client = pg_connect(&conn).await?;
            for set in run_each(&sets) {
                let v = pg_run(&client, sql, set, false, false).await?;
                results.push(batch_update_result(&v));
            }
        } else if is_mysql(&engine) {
            let mut c = my_connect(&conn).await?;
            for set in run_each(&sets) {
                match my_run(&mut c, sql, set, false, false).await {
                    Ok(v) => results.push(batch_update_result(&v)),
                    Err(e) => {
                        let _ = c.disconnect().await;
                        return Err(e);
                    }
                }
            }
            let _ = c.disconnect().await;
        } else {
            return Err(unsupported_engine(&conn.engine));
        }
        Ok(AwsResponse::ok_json(json!({ "updateResults": results })))
    }
}

#[async_trait]
impl AwsService for RdsDataService {
    fn service_name(&self) -> &str {
        "rds-data"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some(action) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        match action {
            "ExecuteStatement" => self.execute_statement(&req).await,
            "BatchExecuteStatement" => self.batch_execute_statement(&req).await,
            "BeginTransaction" => self.begin_transaction(&req).await,
            "CommitTransaction" => {
                self.finish_transaction(&req, "COMMIT", "Transaction Committed")
                    .await
            }
            "RollbackTransaction" => {
                self.finish_transaction(&req, "ROLLBACK", "Rollback Complete")
                    .await
            }
            // ExecuteSql is the deprecated, secret-less precursor to
            // ExecuteStatement and was removed from the public API. Return an
            // honest error rather than a fake success.
            other => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                format!("rds-data operation {other} is not supported"),
            )),
        }
    }
}

fn bad_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg.into())
}

fn not_found_txn(txid: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "BadRequestException",
        format!("Transaction {txid} is not found"),
    )
}

fn unsupported_engine(engine: &str) -> AwsServiceError {
    bad_request(format!("RDS Data API is not supported for engine {engine}"))
}

fn is_mysql(engine_lower: &str) -> bool {
    engine_lower.contains("mysql") || engine_lower.contains("maria")
}

/// AWS transaction ids are long opaque base64-ish tokens; mirror the shape.
fn gen_transaction_id() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 48];
    rand::thread_rng().fill_bytes(&mut bytes);
    b64_encode(&bytes)
}

/// Run the terminal SQL (`COMMIT`/`ROLLBACK`) on a connection taken out of a
/// transaction entry and release it.
async fn finish_held(held: HeldConn, sql_keyword: &str) -> Result<(), AwsServiceError> {
    match held {
        HeldConn::Pg(client) => {
            client
                .batch_execute(sql_keyword)
                .await
                .map_err(|e| bad_request(format!("{e}")))?;
        }
        HeldConn::MySql(mut conn) => {
            use mysql_async::prelude::Queryable;
            conn.query_drop(sql_keyword)
                .await
                .map_err(|e| bad_request(format!("{e}")))?;
            let _ = conn.disconnect().await;
        }
    }
    Ok(())
}

/// Background sweep: roll back and release any transaction idle longer than
/// [`TXN_IDLE_TIMEOUT`], so an abandoned `BeginTransaction` never leaks its
/// backing DB connection forever.
async fn reap_idle_transactions(transactions: TxnMap) {
    let mut tick = tokio::time::interval(Duration::from_secs(30));
    loop {
        tick.tick().await;
        reap_once(&transactions).await;
    }
}

/// Sample the transactions that look idle as of `now`, cloning the `Arc`s so the
/// map lock is dropped before any connection is touched.
async fn collect_stale(transactions: &TxnMap, now: Instant) -> Vec<(String, Arc<TxnEntry>)> {
    let map = transactions.lock().await;
    map.iter()
        .filter(|(_, e)| e.is_reapable(now))
        .map(|(id, e)| (id.clone(), e.clone()))
        .collect()
}

/// Roll back and release each sampled transaction — but only after re-verifying,
/// under the map lock, that it is *still* reapable and still the same entry.
/// `is_reapable` rejects a transaction that a statement has pinned in-flight
/// since the sample (the statement bumps `in_flight` under this same lock, so
/// there is no window where a live statement's transaction looks idle), and the
/// `ptr_eq` guard rejects one that was removed and replaced by a new
/// BeginTransaction reusing the id.
async fn reap_stale(transactions: &TxnMap, stale: Vec<(String, Arc<TxnEntry>)>) {
    for (id, entry) in stale {
        let mut map = transactions.lock().await;
        let still_reapable = entry.is_reapable(Instant::now())
            && map.get(&id).is_some_and(|e| Arc::ptr_eq(e, &entry));
        if !still_reapable {
            continue;
        }
        map.remove(&id);
        drop(map);
        if let Some(held) = entry.conn.lock().await.take() {
            let _ = finish_held(held, "ROLLBACK").await;
        }
    }
}

/// One reaper sweep: sample the idle set, then roll back each after a re-check.
async fn reap_once(transactions: &TxnMap) {
    let stale = collect_stale(transactions, Instant::now()).await;
    reap_stale(transactions, stale).await;
}

/// A single positional SQL parameter resolved from an AWS `SqlParameter`.
enum SqlValue {
    Null,
    Bool(bool),
    Long(i64),
    Double(f64),
    Text(String),
    Blob(Vec<u8>),
}

type Params = Vec<(String, SqlValue)>;

/// Parse `parameters[]` into `(name, value)` pairs in request order.
fn parse_parameters(params: Option<&Value>) -> Params {
    let Some(arr) = params.and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|p| {
            let name = p.get("name").and_then(Value::as_str)?.to_string();
            let v = p.get("value")?;
            let sv = if v.get("isNull").and_then(Value::as_bool) == Some(true) {
                SqlValue::Null
            } else if let Some(b) = v.get("booleanValue").and_then(Value::as_bool) {
                SqlValue::Bool(b)
            } else if let Some(n) = v.get("longValue").and_then(Value::as_i64) {
                SqlValue::Long(n)
            } else if let Some(d) = v.get("doubleValue").and_then(Value::as_f64) {
                SqlValue::Double(d)
            } else if let Some(s) = v.get("stringValue").and_then(Value::as_str) {
                SqlValue::Text(s.to_string())
            } else if let Some(b) = v.get("blobValue").and_then(Value::as_str) {
                // blobValue is base64 in the Data API JSON.
                SqlValue::Blob(b64_decode(b))
            } else {
                SqlValue::Null
            };
            Some((name, sv))
        })
        .collect()
}

/// Parse `parameterSets` (a list of parameter lists) for BatchExecuteStatement.
fn parse_parameter_sets(sets: Option<&Value>) -> Vec<Params> {
    let Some(arr) = sets.and_then(Value::as_array) else {
        return Vec::new();
    };
    arr.iter().map(|s| parse_parameters(Some(s))).collect()
}

/// A batch with no parameter sets still runs the statement once (AWS treats an
/// empty `parameterSets` as a single parameterless execution).
fn run_each(sets: &[Params]) -> Vec<&[(String, SqlValue)]> {
    if sets.is_empty() {
        vec![&[]]
    } else {
        sets.iter().map(Vec::as_slice).collect()
    }
}

/// Substitute AWS `:name` named placeholders with the SQL literal rendering of
/// each parameter. We inline (rather than bind) so the engine parses each value
/// with the *column's* type — binding a Data API `longValue` (i64) to an int4
/// column otherwise fails type-serialization. Data API parameters are trusted
/// server-side fixtures, and results still come back fully typed because we run
/// the literal-substituted SQL through the typed `query` path.
fn inline_params(
    sql: &str,
    params: &[(String, SqlValue)],
    lit: impl Fn(&SqlValue) -> String,
) -> String {
    let mut out = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b':' && i + 1 < bytes.len() && is_ident_start(bytes[i + 1]) {
            let mut j = i + 1;
            while j < bytes.len() && is_ident_char(bytes[j]) {
                j += 1;
            }
            let name = &sql[i + 1..j];
            if let Some((_, v)) = params.iter().find(|(n, _)| n == name) {
                out.push_str(&lit(v));
                i = j;
                continue;
            }
        }
        // Copy one whole UTF-8 character. `:` and identifier bytes are ASCII, so
        // the placeholder scan above only ever fires on a char boundary; here we
        // must advance by the full character width, not a single byte, or
        // multi-byte literals (e.g. 'café') would be corrupted into mojibake.
        let ch = sql[i..]
            .chars()
            .next()
            .expect("byte index is on a char boundary");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Quote an SQL identifier (schema/table name) so a caller-supplied `schema`
/// can't break out of the `SET search_path` statement.
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// SQL literal for postgres.
fn pg_literal(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        SqlValue::Long(n) => n.to_string(),
        SqlValue::Double(d) => d.to_string(),
        SqlValue::Text(s) => sql_quote(s),
        SqlValue::Blob(b) => format!("'\\x{}'::bytea", hex(b)),
    }
}

/// SQL literal for mysql/mariadb.
fn mysql_literal(v: &SqlValue) -> String {
    match v {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        SqlValue::Long(n) => n.to_string(),
        SqlValue::Double(d) => d.to_string(),
        SqlValue::Text(s) => sql_quote(s),
        SqlValue::Blob(b) => format!("x'{}'", hex(b)),
    }
}

/// Is the statement a data-modifying `INSERT`/`UPDATE`/`DELETE ... RETURNING`?
/// These come back through the row-returning path (they have a result set) but,
/// unlike a plain `SELECT`, they also change rows — so `numberOfRecordsUpdated`
/// must reflect the affected count rather than 0.
fn is_returning_dml(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_uppercase();
    (head.starts_with("INSERT") || head.starts_with("UPDATE") || head.starts_with("DELETE"))
        && head.contains(" RETURNING ")
}

/// Build one `updateResults[]` entry for BatchExecuteStatement from a single
/// statement's run output, carrying through any `generatedFields` the write
/// produced (e.g. a MySQL auto-increment id) rather than always emitting `[]`.
fn batch_update_result(run_output: &Value) -> Value {
    let generated = run_output
        .get("generatedFields")
        .cloned()
        .unwrap_or_else(|| json!([]));
    json!({ "generatedFields": generated })
}

/// AWS-shaped `columnMetadata` for a Postgres result description.
fn pg_column_metadata(cols: &[tokio_postgres::Column]) -> Value {
    Value::Array(
        cols.iter()
            .map(|c| {
                json!({
                    "name": c.name(),
                    "label": c.name(),
                    "typeName": c.type_().name(),
                    "nullable": 2, // unknown
                })
            })
            .collect(),
    )
}

/// AWS-shaped `columnMetadata` for a MySQL result description.
fn my_column_metadata(cols: &[mysql_async::Column]) -> Value {
    Value::Array(
        cols.iter()
            .map(|c| {
                json!({
                    "name": c.name_str().to_string(),
                    "label": c.name_str().to_string(),
                    "typeName": format!("{:?}", c.column_type()),
                    "nullable": 2,
                })
            })
            .collect(),
    )
}

/// Does the SQL statement return a result set (vs. an affected-rows write)?
fn returns_rows(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_uppercase();
    head.starts_with("SELECT")
        || head.starts_with("WITH")
        || head.starts_with("SHOW")
        || head.starts_with("VALUES")
        || head.starts_with("TABLE")
        || head.starts_with("EXPLAIN")
        || head.contains(" RETURNING ")
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}
fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

async fn pg_connect(conn: &DbConn) -> Result<tokio_postgres::Client, AwsServiceError> {
    use tokio_postgres::NoTls;
    let cs = format!(
        "host={} port={} user={} password={} dbname={}",
        conn.host, conn.port, conn.user, conn.password, conn.db_name
    );
    let (client, connection) = tokio_postgres::connect(&cs, NoTls)
        .await
        .map_err(|e| bad_request(format!("could not connect to database: {e}")))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    if let Some(schema) = &conn.schema {
        client
            .batch_execute(&format!("SET search_path TO {}", quote_ident(schema)))
            .await
            .map_err(|e| bad_request(format!("could not set schema: {e}")))?;
    }
    Ok(client)
}

async fn pg_run(
    client: &tokio_postgres::Client,
    sql: &str,
    params: &[(String, SqlValue)],
    include_metadata: bool,
    format_json: bool,
) -> Result<Value, AwsServiceError> {
    let stmt = inline_params(sql, params, pg_literal);
    let no_params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[];

    let mut out = Map::new();
    // Route by statement kind so a write runs EXACTLY once (query() executes the
    // statement too, so running execute() after would double-apply it).
    if !returns_rows(&stmt) {
        let affected = client
            .execute(stmt.as_str(), no_params)
            .await
            .map_err(|e| bad_request(format!("{e}")))?;
        out.insert("numberOfRecordsUpdated".into(), json!(affected));
        out.insert("records".into(), json!([]));
        return Ok(Value::Object(out));
    }

    // Prepare first, then run the prepared statement: the `Statement`'s column
    // description is available even when the query returns zero rows, so a
    // `SELECT ... WHERE 1=0` still reports its columns under
    // `includeResultMetadata` (a bare `client.query()` would leave us with no
    // row to read columns from).
    let statement = client
        .prepare(stmt.as_str())
        .await
        .map_err(|e| bad_request(format!("{e}")))?;
    let rows = client
        .query(&statement, no_params)
        .await
        .map_err(|e| bad_request(format!("{e}")))?;
    let cols = statement.columns();

    // A pure SELECT never updates rows (numberOfRecordsUpdated = 0); a DML
    // `... RETURNING` (INSERT/UPDATE/DELETE) reports the affected count, which
    // equals the number of returned rows. Emit the field unconditionally so
    // typed SDKs read a present value rather than defaulting it silently.
    let updated = if is_returning_dml(&stmt) {
        rows.len() as i64
    } else {
        0
    };
    out.insert("numberOfRecordsUpdated".into(), json!(updated));

    if include_metadata {
        out.insert("columnMetadata".into(), pg_column_metadata(cols));
    }

    if rows.is_empty() {
        if format_json {
            out.insert("formattedRecords".into(), json!("[]"));
        } else {
            out.insert("records".into(), json!([]));
        }
        return Ok(Value::Object(out));
    }

    let mut records: Vec<Value> = Vec::with_capacity(rows.len());
    let mut json_rows: Vec<Map<String, Value>> = Vec::new();
    for row in &rows {
        let mut rec: Vec<Value> = Vec::with_capacity(cols.len());
        let mut jr = Map::new();
        for (i, col) in cols.iter().enumerate() {
            let field = pg_field(row, i, col.type_());
            if format_json {
                jr.insert(col.name().to_string(), field_to_plain(&field));
            }
            rec.push(field);
        }
        records.push(Value::Array(rec));
        if format_json {
            json_rows.push(jr);
        }
    }
    if format_json {
        out.insert(
            "formattedRecords".into(),
            json!(serde_json::to_string(&json_rows).unwrap_or_default()),
        );
    } else {
        out.insert("records".into(), Value::Array(records));
    }
    Ok(Value::Object(out))
}

/// Append a fractional-seconds suffix only when non-zero, with trailing zeros
/// trimmed, matching Postgres's own `timestamp::text` rendering.
fn pg_timestamp_text(base: impl std::fmt::Display, micros: u32) -> String {
    if micros == 0 {
        return base.to_string();
    }
    let frac = format!("{micros:06}");
    let frac = frac.trim_end_matches('0');
    format!("{base}.{frac}")
}

fn pg_field(row: &tokio_postgres::Row, i: usize, ty: &tokio_postgres::types::Type) -> Value {
    use tokio_postgres::types::Type;
    macro_rules! opt {
        ($t:ty, $key:expr, $conv:expr) => {{
            let v: Option<$t> = row.try_get(i).unwrap_or(None);
            match v {
                Some(x) => json!({ $key: $conv(x) }),
                None => json!({ "isNull": true }),
            }
        }};
    }
    // Types that AWS surfaces as `stringValue` (numeric/decimal, temporal,
    // UUID, JSON): decode them with their real Rust representation and render
    // the canonical text, rather than letting them fall through to the
    // `String` arm — which fails for these binary-format columns and would
    // wrongly report `isNull`.
    macro_rules! strv {
        ($t:ty, $conv:expr) => {{
            let v: Option<$t> = row.try_get(i).unwrap_or(None);
            match v {
                Some(x) => json!({ "stringValue": $conv(x) }),
                None => json!({ "isNull": true }),
            }
        }};
    }
    match *ty {
        Type::BOOL => opt!(bool, "booleanValue", |x| x),
        Type::INT2 => opt!(i16, "longValue", |x| x as i64),
        Type::INT4 => opt!(i32, "longValue", |x| x as i64),
        Type::INT8 => opt!(i64, "longValue", |x: i64| x),
        Type::FLOAT4 => opt!(f32, "doubleValue", |x| x as f64),
        Type::FLOAT8 => opt!(f64, "doubleValue", |x: f64| x),
        Type::NUMERIC => strv!(rust_decimal::Decimal, |d: rust_decimal::Decimal| d
            .to_string()),
        Type::UUID => strv!(uuid::Uuid, |u: uuid::Uuid| u.to_string()),
        Type::JSON | Type::JSONB => {
            strv!(serde_json::Value, |j: serde_json::Value| j.to_string())
        }
        Type::TIMESTAMP => strv!(chrono::NaiveDateTime, |t: chrono::NaiveDateTime| {
            pg_timestamp_text(
                t.format("%Y-%m-%d %H:%M:%S"),
                t.and_utc().timestamp_subsec_micros(),
            )
        }),
        Type::TIMESTAMPTZ => strv!(chrono::DateTime<chrono::Utc>, |t: chrono::DateTime<
            chrono::Utc,
        >| format!(
            "{}{}",
            pg_timestamp_text(t.format("%Y-%m-%d %H:%M:%S"), t.timestamp_subsec_micros()),
            t.format("%:z")
        )),
        Type::DATE => strv!(chrono::NaiveDate, |d: chrono::NaiveDate| d.to_string()),
        Type::TIME => strv!(chrono::NaiveTime, |t: chrono::NaiveTime| t.to_string()),
        Type::BYTEA => {
            let v: Option<Vec<u8>> = row.try_get(i).unwrap_or(None);
            match v {
                Some(b) => json!({ "blobValue": b64_encode(&b) }),
                None => json!({ "isNull": true }),
            }
        }
        _ => {
            let v: Option<String> = row.try_get(i).unwrap_or(None);
            match v {
                Some(s) => json!({ "stringValue": s }),
                None => json!({ "isNull": true }),
            }
        }
    }
}

async fn my_connect(conn: &DbConn) -> Result<mysql_async::Conn, AwsServiceError> {
    use mysql_async::OptsBuilder;
    let opts = OptsBuilder::default()
        .ip_or_hostname(conn.host.as_str())
        .tcp_port(conn.port)
        .user(Some(&conn.user))
        .pass(Some(&conn.password))
        .db_name(Some(&conn.db_name));
    mysql_async::Conn::new(opts)
        .await
        .map_err(|e| bad_request(format!("could not connect to database: {e}")))
}

async fn my_run(
    c: &mut mysql_async::Conn,
    sql: &str,
    params: &[(String, SqlValue)],
    include_metadata: bool,
    format_json: bool,
) -> Result<Value, AwsServiceError> {
    use mysql_async::prelude::Queryable;
    use mysql_async::{Column, Row};

    let stmt = inline_params(sql, params, mysql_literal);
    let mut out = Map::new();

    // Route writes through `query_drop` so the connection retains the INSERT's
    // OK packet (and thus `last_insert_id`); `query` collecting rows can leave a
    // result-set terminator as the last packet.
    if !returns_rows(&stmt) {
        c.query_drop(stmt.as_str())
            .await
            .map_err(|e| bad_request(format!("{e}")))?;
        out.insert("numberOfRecordsUpdated".into(), json!(c.affected_rows()));
        // AWS Aurora MySQL surfaces the auto-increment key in `generatedFields`
        // (Postgres needs `RETURNING` instead, so its arm omits this). Read it
        // back explicitly on the same connection — robust across the text
        // protocol regardless of which OK packet the driver retained.
        let generated: Option<u64> = c
            .query_first("SELECT LAST_INSERT_ID()")
            .await
            .ok()
            .flatten()
            .filter(|id: &u64| *id != 0);
        if let Some(id) = generated {
            out.insert(
                "generatedFields".into(),
                json!([{ "longValue": id as i64 }]),
            );
        }
        out.insert("records".into(), json!([]));
        return Ok(Value::Object(out));
    }

    // Run through `query_iter` so the result-set column definitions are read off
    // the wire (and captured) before the rows are drained — a zero-row result
    // (`WHERE 1=0`) therefore still exposes its columns under
    // `includeResultMetadata`, which `rows[0].columns()` cannot do when there is
    // no row 0.
    let result = c
        .query_iter(stmt.as_str())
        .await
        .map_err(|e| bad_request(format!("{e}")))?;
    let cols: std::sync::Arc<[Column]> = result
        .columns()
        .unwrap_or_else(|| std::sync::Arc::from(Vec::<Column>::new()));
    let rows: Vec<Row> = result
        .collect_and_drop::<Row>()
        .await
        .map_err(|e| bad_request(format!("{e}")))?;
    let affected = c.affected_rows();

    // A pure SELECT never updates rows; a DML `... RETURNING` (MariaDB) reports
    // the affected count. Keep the field consistent with the Postgres arm.
    let updated = if is_returning_dml(&stmt) {
        affected as i64
    } else {
        0
    };
    out.insert("numberOfRecordsUpdated".into(), json!(updated));

    if include_metadata {
        out.insert("columnMetadata".into(), my_column_metadata(&cols));
    }

    if rows.is_empty() {
        if format_json {
            out.insert("formattedRecords".into(), json!("[]"));
        } else {
            out.insert("records".into(), json!([]));
        }
        return Ok(Value::Object(out));
    }

    let mut records: Vec<Value> = Vec::with_capacity(rows.len());
    let mut json_rows: Vec<Map<String, Value>> = Vec::new();
    for row in &rows {
        let mut rec: Vec<Value> = Vec::with_capacity(cols.len());
        let mut jr = Map::new();
        for (i, col) in cols.iter().enumerate() {
            let field = my_field(row, i, col.column_type());
            if format_json {
                jr.insert(col.name_str().to_string(), field_to_plain(&field));
            }
            rec.push(field);
        }
        records.push(Value::Array(rec));
        if format_json {
            json_rows.push(jr);
        }
    }
    if format_json {
        out.insert(
            "formattedRecords".into(),
            json!(serde_json::to_string(&json_rows).unwrap_or_default()),
        );
    } else {
        out.insert("records".into(), Value::Array(records));
    }
    Ok(Value::Object(out))
}

fn my_field(row: &mysql_async::Row, i: usize, ty: mysql_async::consts::ColumnType) -> Value {
    use mysql_async::Value as V;
    match row.as_ref(i) {
        Some(V::NULL) | None => json!({ "isNull": true }),
        // Binary-protocol typed values (kept for completeness).
        Some(V::Int(n)) => json!({ "longValue": n }),
        Some(V::UInt(n)) => json!({ "longValue": *n as i64 }),
        Some(V::Float(f)) => json!({ "doubleValue": *f as f64 }),
        Some(V::Double(d)) => json!({ "doubleValue": d }),
        // Under the text protocol every column arrives as raw bytes, so coerce
        // numeric columns to the right typed Field by the column's declared
        // type rather than always returning `stringValue`.
        Some(V::Bytes(b)) => my_bytes_field(b, ty),
        // Temporal columns arrive as typed Date/Time values under the binary
        // protocol; render the canonical SQL text instead of the debug form.
        Some(V::Date(y, mo, d, h, mi, s, us)) => {
            json!({ "stringValue": format_mysql_datetime(*y, *mo, *d, *h, *mi, *s, *us) })
        }
        Some(V::Time(neg, days, h, mi, s, us)) => {
            json!({ "stringValue": format_mysql_time(*neg, *days, *h, *mi, *s, *us) })
        }
    }
}

/// Coerce a text-protocol byte column to a typed AWS Field by its MySQL type.
/// Integers -> `longValue`, real/double -> `doubleValue`; decimals, dates,
/// times, and strings stay `stringValue`; non-UTF-8 bytes (true binary/blob)
/// become `blobValue`.
fn my_bytes_field(b: &[u8], ty: mysql_async::consts::ColumnType) -> Value {
    use mysql_async::consts::ColumnType::*;
    let as_str = std::str::from_utf8(b).ok();
    match ty {
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_LONG | MYSQL_TYPE_LONGLONG
        | MYSQL_TYPE_INT24 | MYSQL_TYPE_YEAR => {
            if let Some(n) = as_str.and_then(|s| s.parse::<i64>().ok()) {
                return json!({ "longValue": n });
            }
        }
        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => {
            if let Some(d) = as_str.and_then(|s| s.parse::<f64>().ok()) {
                return json!({ "doubleValue": d });
            }
        }
        _ => {}
    }
    match as_str {
        Some(s) => json!({ "stringValue": s }),
        None => json!({ "blobValue": b64_encode(b) }),
    }
}

/// Render a MySQL `DATE`/`DATETIME`/`TIMESTAMP` value as canonical SQL text,
/// dropping the time portion for a pure date.
#[allow(clippy::too_many_arguments)]
fn format_mysql_datetime(
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    min: u8,
    sec: u8,
    micros: u32,
) -> String {
    let date = format!("{year:04}-{month:02}-{day:02}");
    if hour == 0 && min == 0 && sec == 0 && micros == 0 {
        return date;
    }
    if micros == 0 {
        format!("{date} {hour:02}:{min:02}:{sec:02}")
    } else {
        format!("{date} {hour:02}:{min:02}:{sec:02}.{micros:06}")
    }
}

/// Render a MySQL `TIME` value (a signed duration) as canonical SQL text.
fn format_mysql_time(neg: bool, days: u32, hours: u8, mins: u8, secs: u8, micros: u32) -> String {
    let sign = if neg { "-" } else { "" };
    let total_hours = days * 24 + hours as u32;
    if micros == 0 {
        format!("{sign}{total_hours:02}:{mins:02}:{secs:02}")
    } else {
        format!("{sign}{total_hours:02}:{mins:02}:{secs:02}.{micros:06}")
    }
}

/// Collapse a typed Field to a plain JSON scalar for `formatRecordsAs=JSON`.
fn field_to_plain(field: &Value) -> Value {
    let o = field.as_object();
    if o.and_then(|m| m.get("isNull")).is_some() {
        return Value::Null;
    }
    for k in [
        "stringValue",
        "longValue",
        "doubleValue",
        "booleanValue",
        "blobValue",
    ] {
        if let Some(v) = o.and_then(|m| m.get(k)) {
            return v.clone();
        }
    }
    Value::Null
}

fn b64_encode(b: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(b)
}
fn b64_decode(s: &str) -> Vec<u8> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_returning_dml_matches_only_dml_returning() {
        assert!(is_returning_dml("INSERT INTO t VALUES (1) RETURNING id"));
        assert!(is_returning_dml("  update t set x = 1 returning *"));
        assert!(is_returning_dml("DELETE FROM t WHERE id = 1 RETURNING id"));
        // A plain SELECT is not a write, even when a column is named `returning`.
        assert!(!is_returning_dml("SELECT 1"));
        assert!(!is_returning_dml("SELECT returning_col FROM t"));
        // An INSERT without RETURNING goes through the affected-rows write path.
        assert!(!is_returning_dml("INSERT INTO t VALUES (1)"));
    }

    #[test]
    fn batch_update_result_carries_generated_fields() {
        // The MySQL write path populates generatedFields; the batch entry must
        // surface it rather than discarding it for an empty list.
        let out = json!({
            "numberOfRecordsUpdated": 1,
            "generatedFields": [{ "longValue": 42 }],
            "records": [],
        });
        assert_eq!(
            batch_update_result(&out),
            json!({ "generatedFields": [{ "longValue": 42 }] })
        );
    }

    #[test]
    fn batch_update_result_defaults_to_empty_generated_fields() {
        // A statement that produced no generated key (e.g. Postgres, or a
        // non-auto-increment INSERT) still yields a well-formed entry.
        let out = json!({ "numberOfRecordsUpdated": 1, "records": [] });
        assert_eq!(batch_update_result(&out), json!({ "generatedFields": [] }));
    }

    fn idle_entry(last_used: Instant) -> Arc<TxnEntry> {
        // conn = None so the reaper never tries to touch a real database.
        Arc::new(TxnEntry {
            conn: Mutex::new(None),
            last_used: StdMutex::new(last_used),
            in_flight: AtomicU64::new(0),
        })
    }

    #[tokio::test(start_paused = true)]
    async fn reaper_rolls_back_idle_transaction() {
        let transactions: TxnMap = Arc::new(Mutex::new(HashMap::new()));
        let base = Instant::now();
        transactions
            .lock()
            .await
            .insert("tx1".to_string(), idle_entry(base));
        // Advance past the idle window, then sweep.
        tokio::time::advance(TXN_IDLE_TIMEOUT + Duration::from_secs(1)).await;
        reap_once(&transactions).await;
        assert!(
            transactions.lock().await.is_empty(),
            "a genuinely idle transaction is reaped"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reaper_spares_transaction_with_in_flight_statement() {
        // Reproduces the Cubic P1 race: a statement has taken a reference to the
        // entry (pin) but has NOT yet refreshed last_used (it is a slow query
        // still running). Even though last_used is stale, the in-flight pin must
        // keep the reaper from rolling the live transaction back.
        let transactions: TxnMap = Arc::new(Mutex::new(HashMap::new()));
        let base = Instant::now();
        let entry = idle_entry(base);
        transactions
            .lock()
            .await
            .insert("tx1".to_string(), entry.clone());
        tokio::time::advance(TXN_IDLE_TIMEOUT + Duration::from_secs(1)).await;
        // A statement is now in-flight: pinned, but last_used still points at the
        // pre-statement (stale) instant.
        entry.in_flight.fetch_add(1, Ordering::SeqCst);
        reap_once(&transactions).await;
        assert!(
            transactions.lock().await.contains_key("tx1"),
            "a transaction with an in-flight statement is not reaped despite a stale last_used"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reaper_rechecks_staleness_before_rollback() {
        let transactions: TxnMap = Arc::new(Mutex::new(HashMap::new()));
        let base = Instant::now();
        let entry = idle_entry(base);
        transactions
            .lock()
            .await
            .insert("tx1".to_string(), entry.clone());
        tokio::time::advance(TXN_IDLE_TIMEOUT + Duration::from_secs(1)).await;
        // The reaper samples the entry as stale...
        let stale = collect_stale(&transactions, Instant::now()).await;
        assert_eq!(stale.len(), 1, "entry sampled as stale");
        // ...but an in-flight ExecuteStatement reactivates it before removal.
        entry.touch();
        reap_stale(&transactions, stale).await;
        assert!(
            transactions.lock().await.contains_key("tx1"),
            "a reactivated transaction is NOT rolled back (TOCTOU guarded)"
        );
    }
}
