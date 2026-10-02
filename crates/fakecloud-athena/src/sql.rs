//! Minimal SQL execution for Athena `StartQueryExecution`.
//!
//! Parses the query with `sqlparser` (Generic dialect), resolves the source
//! table via the Glue catalog (cross-account state shared with `fakecloud-glue`),
//! reads the CSV-backed data from S3 (cross-account state shared with
//! `fakecloud-s3`), applies projection / single-equality `WHERE` / `LIMIT`
//! in-memory, and writes the result back to the configured
//! `ResultConfiguration.OutputLocation` as CSV.
//!
//! Scope: enough to back the common `SELECT col FROM db.table WHERE c='v' LIMIT N`
//! shape that real callers use for smoke-tests against Athena. Joins, GROUP BY,
//! aggregates, ORDER BY, subqueries and Parquet/JSON SerDes are out of scope —
//! we return a structured error so callers see a real failure instead of fake
//! data.

use std::collections::BTreeMap;

use bytes::Bytes;
use fakecloud_glue::{SharedGlueState, StorageDescriptor};
use fakecloud_persistence::S3Store;
use fakecloud_s3::body_io::ObjectBodyHandle;
use fakecloud_s3::compression::{decompress, Codec};
use fakecloud_s3::SharedS3State;
use sqlparser::ast::{
    BinaryOperator, Expr, GroupByExpr, LimitClause, ObjectName, Query, SelectItem, SetExpr,
    Statement, TableFactor, Value as SqlValue, ValueWithSpan,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Result of executing a query: in-memory rows ready for `GetQueryResults`,
/// plus the S3 location where we wrote the CSV result.
#[derive(Debug, Clone)]
pub struct ExecutedQuery {
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Vec<String>>,
    pub data_scanned_bytes: i64,
    /// Resolved s3:// URL (`output_location` joined with the result key).
    pub output_location: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("syntax error: {0}")]
    Parse(String),
    #[error("only SELECT statements are supported (got {0})")]
    UnsupportedStatement(&'static str),
    #[error("only single-table SELECTs are supported")]
    UnsupportedJoin,
    #[error("unsupported clause: {0}")]
    UnsupportedClause(&'static str),
    #[error("table reference must be `<database>.<table>` or `<table>` with a default database")]
    InvalidTableRef,
    #[error("database `{0}` not found in catalog")]
    DatabaseNotFound(String),
    #[error("table `{database}.{table}` not found in catalog")]
    TableNotFound { database: String, table: String },
    #[error("table `{database}.{table}` has no Storage.Location")]
    MissingLocation { database: String, table: String },
    #[error("table `{database}.{table}` location `{location}` is not an s3:// URL")]
    InvalidLocation {
        database: String,
        table: String,
        location: String,
    },
    #[error("S3 bucket `{0}` not found")]
    BucketNotFound(String),
    #[error("S3 object body unreadable: {0}")]
    S3Read(String),
    #[error("failed to write query results: {0}")]
    S3Write(String),
    #[error("output location `{0}` is not an s3:// URL")]
    InvalidOutputLocation(String),
    #[error("only CSV-backed Glue tables are supported (got serde `{0}`)")]
    UnsupportedSerde(String),
    #[error("column `{0}` is not in the table schema")]
    UnknownColumn(String),
    #[error("WHERE only supports `<column> = <literal>` for now")]
    UnsupportedWhere,
}

/// Where a query runs: the caller's account/region, the shared Glue catalog
/// and S3 state it reads, the durable S3 store results are written through,
/// and the query execution id that names the result object.
pub struct ExecEnv<'a> {
    pub account_id: &'a str,
    pub region: &'a str,
    pub glue: Option<&'a SharedGlueState>,
    pub s3: Option<&'a SharedS3State>,
    pub s3_store: Option<&'a dyn S3Store>,
    pub query_execution_id: &'a str,
}

/// Parse and execute the query. Returns either the executed result or a
/// structured error so the caller can mark the query `FAILED` with a real
/// reason.
///
/// This does blocking IO (S3 object reads, the result write); run it off the
/// async request path. No S3 lock is held while object bodies are read or the
/// result is written.
pub fn execute(
    query: &str,
    default_database: Option<&str>,
    output_location: Option<&str>,
    env: &ExecEnv<'_>,
) -> Result<ExecutedQuery, SqlError> {
    let ExecEnv {
        account_id,
        region,
        glue,
        s3,
        ..
    } = *env;
    let dialect = GenericDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, query).map_err(|e| SqlError::Parse(e.to_string()))?;
    if statements.len() != 1 {
        return Err(SqlError::Parse(format!(
            "expected 1 statement, got {}",
            statements.len()
        )));
    }
    let stmt = statements.remove(0);
    let select_query = match stmt {
        Statement::Query(q) => q,
        Statement::Insert { .. } => return Err(SqlError::UnsupportedStatement("INSERT")),
        Statement::Update { .. } => return Err(SqlError::UnsupportedStatement("UPDATE")),
        Statement::Delete { .. } => return Err(SqlError::UnsupportedStatement("DELETE")),
        Statement::CreateTable { .. } => {
            return Err(SqlError::UnsupportedStatement("CREATE TABLE"))
        }
        _ => return Err(SqlError::UnsupportedStatement("non-SELECT")),
    };

    let plan = build_plan(&select_query, default_database)?;

    // Literal-only SELECT (no FROM): evaluate inline. Real Athena supports
    // this for `SELECT 1`, `SELECT 'foo'`, etc.
    if plan.source.is_none() {
        return execute_literal_select(&plan, output_location, env);
    }
    let source = plan.source.as_ref().expect("source is Some by check above");

    // Resolve the table via Glue.
    let glue_state = glue.ok_or_else(|| SqlError::TableNotFound {
        database: source.database.clone(),
        table: source.table.clone(),
    })?;
    let table = {
        let g = glue_state.read();
        let acct = g
            .get(account_id)
            .ok_or_else(|| SqlError::DatabaseNotFound(source.database.clone()))?;
        let dbs = acct
            .dbs_in(region)
            .ok_or_else(|| SqlError::DatabaseNotFound(source.database.clone()))?;
        let db = dbs
            .get(&source.database)
            .ok_or_else(|| SqlError::DatabaseNotFound(source.database.clone()))?;
        db.tables
            .get(&source.table)
            .cloned()
            .ok_or_else(|| SqlError::TableNotFound {
                database: source.database.clone(),
                table: source.table.clone(),
            })?
    };

    let storage = table
        .storage_descriptor
        .as_ref()
        .ok_or_else(|| SqlError::MissingLocation {
            database: source.database.clone(),
            table: source.table.clone(),
        })?;

    require_csv_serde(storage)?;

    let location = storage
        .location
        .clone()
        .ok_or_else(|| SqlError::MissingLocation {
            database: source.database.clone(),
            table: source.table.clone(),
        })?;
    let (bucket, prefix) = parse_s3_url(&location).ok_or_else(|| SqlError::InvalidLocation {
        database: source.database.clone(),
        table: source.table.clone(),
        location: location.clone(),
    })?;

    let s3_state = s3.ok_or_else(|| SqlError::BucketNotFound(bucket.clone()))?;

    // Read all CSV rows under the prefix.
    let (raw_rows, scanned) = read_csv_under_prefix(s3_state, account_id, &bucket, &prefix)?;

    let table_columns: Vec<(String, String)> = storage
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.column_type.clone()))
        .collect();

    // Build header mapping: column name -> index. CSVs may or may not have a
    // header; for now we assume no header and assume the column order matches
    // the table's column order.
    let column_index: BTreeMap<String, usize> = table_columns
        .iter()
        .enumerate()
        .map(|(i, (n, _))| (n.to_lowercase(), i))
        .collect();

    // Validate WHERE column.
    if let Some(ref f) = plan.filter {
        if !column_index.contains_key(&f.column.to_lowercase()) {
            return Err(SqlError::UnknownColumn(f.column.clone()));
        }
    }

    // Resolve projection: either * or list of columns.
    let projected: Vec<(String, String)> = match &plan.projection {
        Projection::All => table_columns.clone(),
        Projection::Columns(cols) => cols
            .iter()
            .map(|c| {
                let idx = column_index
                    .get(&c.to_lowercase())
                    .ok_or_else(|| SqlError::UnknownColumn(c.clone()))?;
                Ok((table_columns[*idx].0.clone(), table_columns[*idx].1.clone()))
            })
            .collect::<Result<_, SqlError>>()?,
    };
    let projected_indices: Vec<usize> = projected
        .iter()
        .map(|(name, _)| column_index[&name.to_lowercase()])
        .collect();

    // Apply filter + projection + limit.
    let mut out_rows: Vec<Vec<String>> = Vec::new();
    for raw in &raw_rows {
        if let Some(ref f) = plan.filter {
            let idx = column_index[&f.column.to_lowercase()];
            let cell = raw.get(idx).map(String::as_str).unwrap_or("");
            if cell != f.value {
                continue;
            }
        }
        let mut projected_row = Vec::with_capacity(projected_indices.len());
        for &i in &projected_indices {
            projected_row.push(raw.get(i).cloned().unwrap_or_default());
        }
        out_rows.push(projected_row);
        if let Some(limit) = plan.limit {
            if out_rows.len() as u64 >= limit {
                break;
            }
        }
    }

    // Write CSV result back to OutputLocation if configured.
    let output = match output_location {
        Some(loc) => Some(write_result_csv(env, loc, &projected, &out_rows)?),
        None => None,
    };

    Ok(ExecutedQuery {
        columns: projected,
        rows: out_rows,
        data_scanned_bytes: scanned as i64,
        output_location: output,
    })
}

#[derive(Debug)]
struct Plan {
    /// `None` for literal-only `SELECT 1` style queries (no FROM clause).
    source: Option<TableSource>,
    projection: Projection,
    filter: Option<Filter>,
    limit: Option<u64>,
    /// Raw projection items as parsed; used for literal SELECTs to evaluate
    /// expressions directly (the table-backed path uses `projection`).
    literal_items: Vec<LiteralProjectionItem>,
}

#[derive(Debug, Clone)]
struct TableSource {
    database: String,
    table: String,
}

#[derive(Debug, Clone)]
struct LiteralProjectionItem {
    /// Output column name (`_col0`, alias, or identifier text).
    name: String,
    /// Pre-evaluated string value if this is a literal; `None` means we don't
    /// support evaluating it without a source table.
    value: Option<String>,
    /// Athena type label for `ResultSetMetadata.ColumnInfo`.
    type_label: String,
}

#[derive(Debug)]
enum Projection {
    All,
    Columns(Vec<String>),
}

#[derive(Debug)]
struct Filter {
    column: String,
    value: String,
}

fn build_plan(query: &Query, default_database: Option<&str>) -> Result<Plan, SqlError> {
    if query.with.is_some() {
        return Err(SqlError::UnsupportedClause("WITH"));
    }
    if query.order_by.is_some() {
        return Err(SqlError::UnsupportedClause("ORDER BY"));
    }
    let limit = match &query.limit_clause {
        None => None,
        Some(LimitClause::LimitOffset {
            limit:
                Some(Expr::Value(ValueWithSpan {
                    value: SqlValue::Number(n, _),
                    ..
                })),
            offset,
            limit_by,
        }) if offset.is_none() && limit_by.is_empty() => Some(n.parse::<u64>().map_err(|_| {
            SqlError::Parse(format!("LIMIT must be a non-negative integer (got {n})"))
        })?),
        Some(LimitClause::LimitOffset {
            limit: None,
            offset: None,
            limit_by,
        }) if limit_by.is_empty() => None,
        Some(_) => return Err(SqlError::UnsupportedClause("non-literal LIMIT/OFFSET")),
    };

    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err(SqlError::UnsupportedStatement("set operation")),
    };
    if select.distinct.is_some() {
        return Err(SqlError::UnsupportedClause("DISTINCT"));
    }
    if !matches!(select.group_by, GroupByExpr::Expressions(ref e, _) if e.is_empty()) {
        return Err(SqlError::UnsupportedClause("GROUP BY"));
    }
    if select.having.is_some() {
        return Err(SqlError::UnsupportedClause("HAVING"));
    }
    if !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return Err(SqlError::UnsupportedClause("CLUSTER/DISTRIBUTE/SORT BY"));
    }

    let source = if select.from.is_empty() {
        None
    } else {
        if select.from.len() != 1 {
            return Err(SqlError::UnsupportedJoin);
        }
        let from = &select.from[0];
        if !from.joins.is_empty() {
            return Err(SqlError::UnsupportedJoin);
        }
        let (database, table) = match &from.relation {
            TableFactor::Table { name, .. } => parse_table_ident(name, default_database)?,
            _ => return Err(SqlError::UnsupportedClause("non-table FROM")),
        };
        Some(TableSource { database, table })
    };

    let (projection, literal_items) = if source.is_some() {
        (build_projection(&select.projection)?, Vec::new())
    } else {
        // No FROM clause: skip column-name projection validation entirely;
        // `execute_literal_select` evaluates the items as constant expressions.
        (Projection::All, build_literal_items(&select.projection)?)
    };
    let filter = if source.is_some() {
        match &select.selection {
            None => None,
            Some(expr) => Some(build_filter(expr)?),
        }
    } else {
        if select.selection.is_some() {
            return Err(SqlError::UnsupportedClause("WHERE without FROM"));
        }
        None
    };

    Ok(Plan {
        source,
        projection,
        filter,
        limit,
        literal_items,
    })
}

fn build_literal_items(items: &[SelectItem]) -> Result<Vec<LiteralProjectionItem>, SqlError> {
    let mut out = Vec::new();
    for (idx, item) in items.iter().enumerate() {
        let (alias_opt, expr) = match item {
            SelectItem::UnnamedExpr(e) => (None, e),
            SelectItem::ExprWithAlias { expr, alias } => (Some(alias.value.clone()), expr),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                return Err(SqlError::UnsupportedClause("`*` without FROM"));
            }
        };
        let (value, type_label) = literal_value(expr)?;
        let name = alias_opt.unwrap_or_else(|| match expr {
            Expr::Identifier(id) => id.value.clone(),
            _ => format!("_col{idx}"),
        });
        out.push(LiteralProjectionItem {
            name,
            value: Some(value),
            type_label,
        });
    }
    Ok(out)
}

fn literal_value(expr: &Expr) -> Result<(String, String), SqlError> {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            SqlValue::Number(n, _) => Ok((n.clone(), "integer".to_string())),
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok((s.clone(), "varchar".to_string()))
            }
            SqlValue::Boolean(b) => Ok((b.to_string(), "boolean".to_string())),
            SqlValue::Null => Ok((String::new(), "varchar".to_string())),
            _ => Err(SqlError::UnsupportedClause("non-literal expression")),
        },
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => {
            let (v, t) = literal_value(expr)?;
            Ok((format!("-{v}"), t))
        }
        _ => Err(SqlError::UnsupportedClause("non-literal expression")),
    }
}

fn execute_literal_select(
    plan: &Plan,
    output_location: Option<&str>,
    env: &ExecEnv<'_>,
) -> Result<ExecutedQuery, SqlError> {
    let columns: Vec<(String, String)> = plan
        .literal_items
        .iter()
        .map(|i| (i.name.clone(), i.type_label.clone()))
        .collect();
    let row: Vec<String> = plan
        .literal_items
        .iter()
        .map(|i| i.value.clone().unwrap_or_default())
        .collect();
    let rows = if plan.limit == Some(0) {
        vec![]
    } else {
        vec![row]
    };

    let output = match (output_location, env.s3) {
        (Some(loc), Some(_)) => Some(write_result_csv(env, loc, &columns, &rows)?),
        _ => None,
    };
    Ok(ExecutedQuery {
        columns,
        rows,
        data_scanned_bytes: 0,
        output_location: output,
    })
}

fn build_projection(items: &[SelectItem]) -> Result<Projection, SqlError> {
    if items.iter().any(|i| matches!(i, SelectItem::Wildcard(_))) {
        if items.len() == 1 {
            return Ok(Projection::All);
        }
        return Err(SqlError::UnsupportedClause("mixed `*` and named columns"));
    }
    let mut cols = Vec::with_capacity(items.len());
    for item in items {
        match item {
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => cols.push(id.value.clone()),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                let last = parts
                    .last()
                    .ok_or_else(|| SqlError::Parse("empty compound identifier".into()))?;
                cols.push(last.value.clone());
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(id),
                ..
            } => cols.push(id.value.clone()),
            _ => return Err(SqlError::UnsupportedClause("non-column select item")),
        }
    }
    Ok(Projection::Columns(cols))
}

fn build_filter(expr: &Expr) -> Result<Filter, SqlError> {
    let (left, op, right) = match expr {
        Expr::BinaryOp { left, op, right } => (left.as_ref(), op, right.as_ref()),
        _ => return Err(SqlError::UnsupportedWhere),
    };
    if !matches!(op, BinaryOperator::Eq) {
        return Err(SqlError::UnsupportedWhere);
    }
    let column = match left {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .ok_or(SqlError::UnsupportedWhere)?
            .value
            .clone(),
        _ => return Err(SqlError::UnsupportedWhere),
    };
    let value = match right {
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => s.clone(),
            SqlValue::Number(n, _) => n.clone(),
            SqlValue::Boolean(b) => b.to_string(),
            _ => return Err(SqlError::UnsupportedWhere),
        },
        _ => return Err(SqlError::UnsupportedWhere),
    };
    Ok(Filter { column, value })
}

fn parse_table_ident(
    name: &ObjectName,
    default_database: Option<&str>,
) -> Result<(String, String), SqlError> {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|p| p.as_ident().map(|i| i.value.clone()))
        .collect();
    match parts.as_slice() {
        [t] => default_database
            .map(|d| (d.to_string(), t.clone()))
            .ok_or(SqlError::InvalidTableRef),
        [d, t] => Ok((d.clone(), t.clone())),
        [_, d, t] => Ok((d.clone(), t.clone())), // catalog.db.table
        _ => Err(SqlError::InvalidTableRef),
    }
}

fn require_csv_serde(storage: &StorageDescriptor) -> Result<(), SqlError> {
    let lib = storage
        .serde_info
        .as_ref()
        .and_then(|s| s.serialization_library.as_deref())
        .unwrap_or("");
    let lib_lower = lib.to_lowercase();
    // Accept the common Hadoop CSV/LazySimple SerDe names; reject Parquet/JSON
    // up front so callers see why the query failed instead of getting silently
    // wrong data.
    let csv_serdes = ["lazysimpleserde", "opencsvserde"];
    if lib.is_empty() || csv_serdes.iter().any(|s| lib_lower.contains(s)) {
        Ok(())
    } else {
        Err(SqlError::UnsupportedSerde(lib.to_string()))
    }
}

fn parse_s3_url(url: &str) -> Option<(String, String)> {
    let rest = url.strip_prefix("s3://")?;
    let (bucket, key_prefix) = match rest.split_once('/') {
        Some((b, k)) => (b.to_string(), k.to_string()),
        None => (rest.to_string(), String::new()),
    };
    Some((bucket, key_prefix))
}

fn read_csv_under_prefix(
    s3: &SharedS3State,
    account_id: &str,
    bucket_name: &str,
    prefix: &str,
) -> Result<(Vec<Vec<String>>, usize), SqlError> {
    // Resolve the objects and open their bodies under the read guard (only
    // metadata work and fd opens), then read and decode them after the guard
    // drops so a large table scan never stalls S3 writers.
    let mut objects: Vec<(String, Codec, ObjectBodyHandle)> = Vec::new();
    {
        let s3_guard = s3.read();
        let acct = s3_guard
            .get(account_id)
            .ok_or_else(|| SqlError::BucketNotFound(bucket_name.to_string()))?;
        let bucket = acct
            .buckets
            .get(bucket_name)
            .ok_or_else(|| SqlError::BucketNotFound(bucket_name.to_string()))?;

        // Sort keys so result ordering is deterministic across re-runs.
        let mut keys: Vec<&String> = bucket
            .objects
            .keys()
            .filter(|k| prefix.is_empty() || k.starts_with(prefix))
            .filter(|k| !is_hidden_data_file(k))
            .collect();
        keys.sort();

        for key in keys {
            let obj = &bucket.objects[key];
            if obj.is_delete_marker {
                continue;
            }
            let handle = ObjectBodyHandle::open(&obj.body)
                .map_err(|e| SqlError::S3Read(format!("{key}: {e}")))?;
            let codec = Codec::from_key(key, obj.content_encoding.as_deref());
            objects.push((key.clone(), codec, handle));
        }
    }

    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut scanned: usize = 0;
    for (key, codec, handle) in objects {
        let raw = handle
            .read_all()
            .map_err(|e| SqlError::S3Read(format!("{key}: {e}")))?;
        // DataScannedInBytes counts the stored (compressed) bytes, as Athena
        // bills them.
        scanned += raw.len();
        let text = if codec == Codec::None {
            raw
        } else {
            Bytes::from(
                decompress(codec, &raw)
                    .map_err(|e| SqlError::S3Read(format!("{key}: cannot decompress: {e}")))?,
            )
        };
        for line in csv_lines(&text) {
            rows.push(parse_csv_line(&line));
        }
    }
    Ok((rows, scanned))
}

/// Athena skips files whose name starts with `_` or `.` (Hadoop/Hive
/// convention for markers such as `_SUCCESS` and temporary files), and
/// zero-length "folder" placeholder keys ending in `/`.
fn is_hidden_data_file(key: &str) -> bool {
    if key.ends_with('/') {
        return true;
    }
    let name = key.rsplit('/').next().unwrap_or(key);
    name.starts_with('_') || name.starts_with('.')
}

/// Split bytes into lines stripping trailing CR/LF; skip empty lines.
fn csv_lines(bytes: &Bytes) -> Vec<String> {
    let text = match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    text.lines()
        .map(str::to_string)
        .filter(|l| !l.is_empty())
        .collect()
}

/// Parse one CSV line. Honors `"`-quoted fields (with `""` escape) and a
/// comma delimiter. Enough for the smoke-test corpus we serve; not RFC 4180
/// complete (no embedded newlines inside quoted fields).
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            }
            '"' => in_quotes = true,
            ',' if !in_quotes => {
                out.push(std::mem::take(&mut current));
            }
            other => current.push(other),
        }
    }
    out.push(current);
    out
}

fn write_result_csv(
    env: &ExecEnv<'_>,
    output_location: &str,
    columns: &[(String, String)],
    rows: &[Vec<String>],
) -> Result<String, SqlError> {
    let (bucket_name, prefix) = parse_s3_url(output_location)
        .ok_or_else(|| SqlError::InvalidOutputLocation(output_location.to_string()))?;
    let s3 = env
        .s3
        .ok_or_else(|| SqlError::BucketNotFound(bucket_name.clone()))?;
    let mut prefix = prefix;
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }
    // Athena names the result file after the query execution id.
    let key = format!("{prefix}{}.csv", env.query_execution_id);

    let mut body = String::new();
    body.push_str(
        &columns
            .iter()
            .map(|(n, _)| escape_csv(n))
            .collect::<Vec<_>>()
            .join(","),
    );
    body.push('\n');
    for row in rows {
        body.push_str(
            &row.iter()
                .map(|v| escape_csv(v))
                .collect::<Vec<_>>()
                .join(","),
        );
        body.push('\n');
    }

    // Written through the durable S3 store (a disk file in persistent mode),
    // so query results survive a restart like any other object.
    fakecloud_s3::delivery::put_object_via_store(
        s3,
        env.s3_store,
        env.account_id,
        &bucket_name,
        &key,
        body.into_bytes(),
        Some("text/csv"),
    )
    .map_err(|e| {
        if e.starts_with("bucket ") {
            SqlError::BucketNotFound(bucket_name.clone())
        } else {
            SqlError::S3Write(e)
        }
    })?;
    Ok(format!("s3://{bucket_name}/{key}"))
}

fn escape_csv(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        let escaped = field.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        field.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_select_star_with_db_table() {
        let dialect = GenericDialect {};
        let mut stmts = Parser::parse_sql(&dialect, "SELECT * FROM db.t").unwrap();
        let q = match stmts.remove(0) {
            Statement::Query(q) => q,
            _ => panic!(),
        };
        let plan = build_plan(&q, None).unwrap();
        let src = plan.source.unwrap();
        assert_eq!(src.database, "db");
        assert_eq!(src.table, "t");
        assert!(matches!(plan.projection, Projection::All));
    }

    #[test]
    fn parses_columns_where_limit() {
        let dialect = GenericDialect {};
        let mut stmts =
            Parser::parse_sql(&dialect, "SELECT a, b FROM db.t WHERE a = 'x' LIMIT 2").unwrap();
        let q = match stmts.remove(0) {
            Statement::Query(q) => q,
            _ => panic!(),
        };
        let plan = build_plan(&q, None).unwrap();
        assert!(
            matches!(plan.projection, Projection::Columns(ref c) if c == &vec!["a".to_string(), "b".to_string()])
        );
        let f = plan.filter.unwrap();
        assert_eq!(f.column, "a");
        assert_eq!(f.value, "x");
        assert_eq!(plan.limit, Some(2));
    }

    #[test]
    fn parses_select_literal() {
        let result = execute(
            "SELECT 1",
            None,
            None,
            &ExecEnv {
                account_id: "111111111111",
                region: "us-east-1",
                glue: None,
                s3: None,
                s3_store: None,
                query_execution_id: "q",
            },
        );
        let exec = result.expect("execute");
        assert_eq!(exec.rows.len(), 1);
        assert_eq!(exec.rows[0], vec!["1".to_string()]);
        assert_eq!(exec.columns[0].1, "integer");
    }

    #[test]
    fn rejects_join() {
        let dialect = GenericDialect {};
        let mut stmts =
            Parser::parse_sql(&dialect, "SELECT * FROM db.t JOIN db.u ON t.id = u.id").unwrap();
        let q = match stmts.remove(0) {
            Statement::Query(q) => q,
            _ => panic!(),
        };
        assert!(matches!(
            build_plan(&q, None),
            Err(SqlError::UnsupportedJoin)
        ));
    }

    #[test]
    fn parse_csv_line_handles_quotes() {
        assert_eq!(
            parse_csv_line("a,\"b,c\",d"),
            vec!["a".to_string(), "b,c".to_string(), "d".to_string()],
        );
        assert_eq!(
            parse_csv_line("a,\"he said \"\"hi\"\"\",b"),
            vec![
                "a".to_string(),
                "he said \"hi\"".to_string(),
                "b".to_string()
            ],
        );
    }

    #[test]
    fn parse_s3_url_splits_bucket_and_prefix() {
        assert_eq!(
            parse_s3_url("s3://bkt/path/to/data/"),
            Some(("bkt".into(), "path/to/data/".into()))
        );
        assert_eq!(parse_s3_url("s3://bkt"), Some(("bkt".into(), "".into())));
        assert_eq!(parse_s3_url("https://example.com"), None);
    }

    #[test]
    fn escape_csv_quotes_when_needed() {
        assert_eq!(escape_csv("plain"), "plain");
        assert_eq!(escape_csv("a,b"), "\"a,b\"");
        assert_eq!(escape_csv("a\"b"), "\"a\"\"b\"");
    }

    fn s3_with_bucket(bucket: &str) -> SharedS3State {
        let mut mas: fakecloud_core::multi_account::MultiAccountState<fakecloud_s3::S3State> =
            fakecloud_core::multi_account::MultiAccountState::new("111111111111", "us-east-1", "");
        mas.get_or_create("111111111111").buckets.insert(
            bucket.to_string(),
            fakecloud_s3::S3Bucket::new(bucket, "us-east-1", "owner"),
        );
        std::sync::Arc::new(parking_lot::RwLock::new(mas))
    }

    fn put(s3: &SharedS3State, bucket: &str, key: &str, body: Vec<u8>) {
        fakecloud_s3::delivery::put_object_via_store(
            s3,
            None,
            "111111111111",
            bucket,
            key,
            body,
            None,
        )
        .unwrap();
    }

    #[test]
    fn reads_compressed_objects_and_skips_marker_files() {
        use std::io::Write;
        let s3 = s3_with_bucket("data");
        put(&s3, "data", "t/a.csv", b"1,plain\n".to_vec());
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(b"2,gzipped\n3,gzipped\n").unwrap();
        let gz = gz.finish().unwrap();
        let gz_len = gz.len();
        put(&s3, "data", "t/b.csv.gz", gz);
        put(&s3, "data", "t/_SUCCESS", b"garbage,row\n".to_vec());
        put(&s3, "data", "t/.tmp-part", b"garbage,row\n".to_vec());

        let (rows, scanned) = read_csv_under_prefix(&s3, "111111111111", "data", "t/").unwrap();
        assert_eq!(
            rows,
            vec![
                vec!["1".to_string(), "plain".to_string()],
                vec!["2".to_string(), "gzipped".to_string()],
                vec!["3".to_string(), "gzipped".to_string()],
            ]
        );
        // Scanned bytes are the stored (compressed) sizes.
        assert_eq!(scanned, "1,plain\n".len() + gz_len);
    }

    #[test]
    fn corrupt_compressed_object_fails_the_query() {
        let s3 = s3_with_bucket("data");
        put(&s3, "data", "t/bad.csv.gz", b"not gzip".to_vec());
        assert!(matches!(
            read_csv_under_prefix(&s3, "111111111111", "data", "t/"),
            Err(SqlError::S3Read(_))
        ));
    }

    #[test]
    fn result_csv_is_named_after_the_query_and_persisted_through_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = fakecloud_persistence::s3::DiskS3Store::new(
            dir.path().to_path_buf(),
            std::sync::Arc::new(fakecloud_persistence::cache::BodyCache::new(0)),
        );
        let s3 = s3_with_bucket("results");
        let env = ExecEnv {
            account_id: "111111111111",
            region: "us-east-1",
            glue: None,
            s3: Some(&s3),
            s3_store: Some(&store),
            query_execution_id: "qe-123",
        };
        let exec = execute("SELECT 1 AS one", None, Some("s3://results/out"), &env).unwrap();
        assert_eq!(
            exec.output_location.as_deref(),
            Some("s3://results/out/qe-123.csv")
        );
        let guard = s3.read();
        let obj = &guard.get("111111111111").unwrap().buckets["results"].objects["out/qe-123.csv"];
        let fakecloud_persistence::BodyRef::Disk { path, .. } = &obj.body else {
            panic!("result must be written through the disk store, got memory body");
        };
        assert_eq!(std::fs::read_to_string(path).unwrap(), "one\n1\n");
    }

    #[test]
    fn missing_result_bucket_fails_the_query() {
        let s3 = s3_with_bucket("other");
        let env = ExecEnv {
            account_id: "111111111111",
            region: "us-east-1",
            glue: None,
            s3: Some(&s3),
            s3_store: None,
            query_execution_id: "qe-1",
        };
        assert!(matches!(
            execute("SELECT 1", None, Some("s3://nope/out/"), &env),
            Err(SqlError::BucketNotFound(_))
        ));
    }
}
