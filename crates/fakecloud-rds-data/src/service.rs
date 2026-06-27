//! RDS Data API (`rds-data`) restJson1 dispatch + real SQL execution.

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Map, Value};

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

/// Connection parameters captured from a resolved `DbInstance` (owned so the
/// state read-lock is dropped before we touch the network).
struct DbConn {
    engine: String,
    host: String,
    port: u16,
    user: String,
    password: String,
    db_name: String,
}

pub struct RdsDataService {
    rds_state: SharedRdsState,
}

impl RdsDataService {
    pub fn new(rds_state: SharedRdsState) -> Self {
        Self { rds_state }
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
            host: resolve_db_host(),
            port: inst.host_port,
            user: inst.master_username.clone(),
            password: inst.master_user_password.clone(),
            db_name: inst.db_name.clone().unwrap_or_default(),
        })
    }

    async fn execute_statement(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = body
            .get("resourceArn")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("resourceArn is required"))?;
        let sql = body
            .get("sql")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("sql is required"))?;
        // secretArn is required by AWS but fakecloud trusts the resourceArn for
        // credential resolution (the secret would carry the same master creds).
        if body.get("secretArn").and_then(Value::as_str).is_none() {
            return Err(bad_request("secretArn is required"));
        }
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

        let conn = self
            .resolve_conn(&req.account_id, resource_arn)
            .ok_or_else(|| {
                bad_request(format!(
                    "HttpEndpoint is not enabled for resource {resource_arn} (no such DB)"
                ))
            })?;

        let engine = conn.engine.to_lowercase();
        let result = if engine.contains("postgres") {
            pg_execute(&conn, sql, &params, include_metadata, format_json).await
        } else if engine.contains("mysql") || engine.contains("maria") {
            mysql_execute(&conn, sql, &params, include_metadata, format_json).await
        } else {
            return Err(bad_request(format!(
                "RDS Data API is not supported for engine {}",
                conn.engine
            )));
        };
        result.map(AwsResponse::ok_json)
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
            // Transactions + batch land in a later batch; return an honest
            // not-implemented rather than a fake success.
            other => Err(AwsServiceError::aws_error(
                StatusCode::NOT_IMPLEMENTED,
                "BadRequestException",
                format!("rds-data operation {other} is not yet implemented"),
            )),
        }
    }
}

fn bad_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg.into())
}

/// The host fakecloud's RDS containers are reachable at (sibling-container aware).
fn resolve_db_host() -> String {
    let cli =
        fakecloud_core::container_net::detect_container_cli().unwrap_or_else(|| "docker".into());
    fakecloud_core::container_net::HostNetworking::detect(&cli).sibling_host
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

/// Parse `parameters[]` into `(name, value)` pairs in request order.
fn parse_parameters(params: Option<&Value>) -> Vec<(String, SqlValue)> {
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
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn sql_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
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

async fn pg_execute(
    conn: &DbConn,
    sql: &str,
    params: &[(String, SqlValue)],
    include_metadata: bool,
    format_json: bool,
) -> Result<Value, AwsServiceError> {
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

    let rows = client
        .query(stmt.as_str(), no_params)
        .await
        .map_err(|e| bad_request(format!("{e}")))?;
    if rows.is_empty() {
        out.insert("numberOfRecordsUpdated".into(), json!(0));
        out.insert("records".into(), json!([]));
        return Ok(Value::Object(out));
    }

    let cols = rows[0].columns();
    if include_metadata {
        let md: Vec<Value> = cols
            .iter()
            .map(|c| {
                json!({
                    "name": c.name(),
                    "label": c.name(),
                    "typeName": c.type_().name(),
                    "nullable": 2, // unknown
                })
            })
            .collect();
        out.insert("columnMetadata".into(), Value::Array(md));
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
    match *ty {
        Type::BOOL => opt!(bool, "booleanValue", |x| x),
        Type::INT2 => opt!(i16, "longValue", |x| x as i64),
        Type::INT4 => opt!(i32, "longValue", |x| x as i64),
        Type::INT8 => opt!(i64, "longValue", |x: i64| x),
        Type::FLOAT4 => opt!(f32, "doubleValue", |x| x as f64),
        Type::FLOAT8 => opt!(f64, "doubleValue", |x: f64| x),
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

async fn mysql_execute(
    conn: &DbConn,
    sql: &str,
    params: &[(String, SqlValue)],
    include_metadata: bool,
    format_json: bool,
) -> Result<Value, AwsServiceError> {
    use mysql_async::prelude::*;
    use mysql_async::{Column, OptsBuilder, Row};

    let opts = OptsBuilder::default()
        .ip_or_hostname(conn.host.as_str())
        .tcp_port(conn.port)
        .user(Some(&conn.user))
        .pass(Some(&conn.password))
        .db_name(Some(&conn.db_name));
    let mut c = mysql_async::Conn::new(opts)
        .await
        .map_err(|e| bad_request(format!("could not connect to database: {e}")))?;

    let stmt = inline_params(sql, params, mysql_literal);
    let rows: Vec<Row> = c
        .query(stmt.as_str())
        .await
        .map_err(|e| bad_request(format!("{e}")))?;
    let affected = c.affected_rows();
    let _ = c.disconnect().await;

    let mut out = Map::new();
    if rows.is_empty() {
        out.insert("numberOfRecordsUpdated".into(), json!(affected));
        out.insert("records".into(), json!([]));
        return Ok(Value::Object(out));
    }

    let cols: std::sync::Arc<[Column]> = rows[0].columns();
    if include_metadata {
        let md: Vec<Value> = cols
            .iter()
            .map(|c| {
                json!({
                    "name": c.name_str().to_string(),
                    "label": c.name_str().to_string(),
                    "typeName": format!("{:?}", c.column_type()),
                    "nullable": 2,
                })
            })
            .collect();
        out.insert("columnMetadata".into(), Value::Array(md));
    }

    let mut records: Vec<Value> = Vec::with_capacity(rows.len());
    let mut json_rows: Vec<Map<String, Value>> = Vec::new();
    for row in &rows {
        let mut rec: Vec<Value> = Vec::with_capacity(cols.len());
        let mut jr = Map::new();
        for (i, col) in cols.iter().enumerate() {
            let field = my_field(row, i);
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

fn my_field(row: &mysql_async::Row, i: usize) -> Value {
    use mysql_async::Value as V;
    match row.as_ref(i) {
        Some(V::NULL) | None => json!({ "isNull": true }),
        Some(V::Int(n)) => json!({ "longValue": n }),
        Some(V::UInt(n)) => json!({ "longValue": *n as i64 }),
        Some(V::Float(f)) => json!({ "doubleValue": *f as f64 }),
        Some(V::Double(d)) => json!({ "doubleValue": d }),
        Some(V::Bytes(b)) => match std::str::from_utf8(b) {
            Ok(s) => json!({ "stringValue": s }),
            Err(_) => json!({ "blobValue": b64_encode(b) }),
        },
        Some(other) => json!({ "stringValue": format!("{other:?}") }),
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
