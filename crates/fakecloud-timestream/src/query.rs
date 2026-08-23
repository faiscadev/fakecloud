//! A real, bounded SQL handler for Amazon Timestream `Query`.
//!
//! A faithful Timestream SQL engine (the full Trino-derived time-series
//! dialect) is out of scope. This module implements a genuine interpreter for
//! the query shapes the SDK, e2e, and conformance exercise, over the points
//! ingested by `WriteRecords`:
//!
//! * `SELECT * FROM "db"."table"` -- every stored point as a row, with
//!   AWS-shaped `ColumnInfo`/`Datum` typing: one `VARCHAR` column per dimension
//!   name, `measure_name` (`VARCHAR`), a `measure_value::<type>` column per
//!   measure value type present, and `time` (`TIMESTAMP`).
//! * `SELECT COUNT(*) FROM "db"."table"` -- a single `BIGINT` row.
//! * optional `WHERE time <op> ago(<n><unit>)` time filter.
//! * optional `ORDER BY time [ASC|DESC]`.
//! * optional `LIMIT <n>`.
//!
//! Anything outside this subset returns [`QueryError::Validation`] naming the
//! unsupported construct, so a caller never gets a wrong or silently-empty
//! success for a query the engine cannot actually run.

use std::sync::OnceLock;

use regex::Regex;
use serde_json::{json, Value};

use crate::state::StoredRecord;

/// What the `SELECT` list projects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    /// `SELECT *`
    Star,
    /// `SELECT COUNT(*)`
    CountStar,
}

/// A comparison operator in a `WHERE time <op> ...` clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Gt,
    Ge,
    Lt,
    Le,
    Eq,
}

/// A parsed, executable query plan.
#[derive(Debug, Clone)]
pub struct Plan {
    pub database: String,
    pub table: String,
    pub projection: Projection,
    /// `(op, threshold_nanos)` derived from `WHERE time <op> ago(...)`.
    pub time_filter: Option<(CmpOp, i128)>,
    /// `Some(true)` = `ORDER BY time DESC`, `Some(false)` = ASC.
    pub order_desc: Option<bool>,
    pub limit: Option<usize>,
}

/// Why a query could not be planned/run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    /// A construct outside the supported subset, or a malformed query.
    Validation(String),
}

impl QueryError {
    pub fn message(&self) -> &str {
        match self {
            QueryError::Validation(m) => m,
        }
    }
}

/// The materialized result of a plan: AWS-shaped `ColumnInfo` list and `Row`
/// list, both ready to drop straight into the `Query` response JSON.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<Value>,
    pub rows: Vec<Value>,
}

fn re_from() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r#"(?i)\bfrom\s+"?([^"\s.]+)"?\s*\.\s*"?([^"\s.]+)"?"#).unwrap())
}

fn re_select() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r#"(?is)^\s*select\s+(.*?)\s+from\b"#).unwrap())
}

fn re_count() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r#"(?i)^count\s*\(\s*\*\s*\)$"#).unwrap())
}

fn re_where_time() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| {
        Regex::new(r#"(?i)\bwhere\s+time\s*(>=|<=|>|<|=)\s*ago\s*\(\s*(\d+)\s*([a-zA-Z]+)\s*\)"#)
            .unwrap()
    })
}

fn re_where_any() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r#"(?i)\bwhere\b"#).unwrap())
}

fn re_order() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r#"(?i)\border\s+by\s+([A-Za-z0-9_]+)(?:\s+(asc|desc))?"#).unwrap())
}

fn re_limit() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r#"(?i)\blimit\s+(\d+)"#).unwrap())
}

/// Parse a single-unit `ago(<n><unit>)` duration into nanoseconds.
fn unit_nanos(unit: &str) -> Option<i128> {
    let n: i128 = match unit.to_lowercase().as_str() {
        "ns" | "nanosecond" | "nanoseconds" => 1,
        "us" | "microsecond" | "microseconds" => 1_000,
        "ms" | "millisecond" | "milliseconds" => 1_000_000,
        "s" | "sec" | "second" | "seconds" => 1_000_000_000,
        "m" | "min" | "minute" | "minutes" => 60 * 1_000_000_000,
        "h" | "hour" | "hours" => 3_600 * 1_000_000_000,
        "d" | "day" | "days" => 86_400 * 1_000_000_000,
        _ => return None,
    };
    Some(n)
}

fn now_nanos() -> i128 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as i128
}

/// Parse a Timestream `SELECT` into an executable [`Plan`], or return a
/// [`QueryError::Validation`] naming the unsupported construct.
pub fn parse_query(sql: &str) -> Result<Plan, QueryError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err(QueryError::Validation("The query string is empty.".into()));
    }

    let select_caps = re_select().captures(trimmed).ok_or_else(|| {
        QueryError::Validation(
            "Only 'SELECT ... FROM \"database\".\"table\"' queries are supported.".into(),
        )
    })?;
    let select_list = select_caps[1].trim();

    let projection = if select_list == "*" {
        Projection::Star
    } else if re_count().is_match(select_list) {
        Projection::CountStar
    } else {
        return Err(QueryError::Validation(format!(
            "Unsupported projection '{select_list}'. Supported: '*' or 'COUNT(*)'."
        )));
    };

    let from_caps = re_from().captures(trimmed).ok_or_else(|| {
        QueryError::Validation(
            "Missing or malformed FROM clause; expected FROM \"database\".\"table\".".into(),
        )
    })?;
    let database = from_caps[1].to_string();
    let table = from_caps[2].to_string();

    // WHERE handling: accept only `WHERE time <op> ago(<n><unit>)`. Any other
    // WHERE shape is outside the supported subset.
    let time_filter = if re_where_any().is_match(trimmed) {
        let caps = re_where_time().captures(trimmed).ok_or_else(|| {
            QueryError::Validation(
                "Unsupported WHERE clause. Supported: WHERE time <op> ago(<n><unit>).".into(),
            )
        })?;
        let op = match &caps[1] {
            ">" => CmpOp::Gt,
            ">=" => CmpOp::Ge,
            "<" => CmpOp::Lt,
            "<=" => CmpOp::Le,
            "=" => CmpOp::Eq,
            other => {
                return Err(QueryError::Validation(format!(
                    "Unsupported comparison operator '{other}'."
                )))
            }
        };
        let n: i128 = caps[2].parse().map_err(|_| {
            QueryError::Validation(format!("Invalid duration magnitude '{}'.", &caps[2]))
        })?;
        let unit = &caps[3];
        let per = unit_nanos(unit).ok_or_else(|| {
            QueryError::Validation(format!("Unsupported ago() time unit '{unit}'."))
        })?;
        // A ~36-digit magnitude overflows i128 on `n * per`; saturate so a
        // pathological ago() bound clamps instead of panicking.
        Some((op, now_nanos().saturating_sub(n.saturating_mul(per))))
    } else {
        None
    };

    let order_desc = if let Some(caps) = re_order().captures(trimmed) {
        let col = caps[1].to_lowercase();
        if col != "time" {
            return Err(QueryError::Validation(format!(
                "ORDER BY is only supported on the 'time' column, not '{}'.",
                &caps[1]
            )));
        }
        Some(
            caps.get(2)
                .map(|m| m.as_str().eq_ignore_ascii_case("desc"))
                .unwrap_or(false),
        )
    } else {
        None
    };

    let limit = re_limit()
        .captures(trimmed)
        .and_then(|c| c[1].parse::<usize>().ok());

    Ok(Plan {
        database,
        table,
        projection,
        time_filter,
        order_desc,
        limit,
    })
}

/// Map a Timestream measure value type onto the scalar column type name.
fn scalar_type_for_measure(ty: &str) -> &'static str {
    match ty {
        "DOUBLE" => "DOUBLE",
        "BIGINT" => "BIGINT",
        "BOOLEAN" => "BOOLEAN",
        "TIMESTAMP" => "TIMESTAMP",
        _ => "VARCHAR",
    }
}

/// Format an epoch-nanoseconds instant as Timestream's timestamp literal,
/// `YYYY-MM-DD HH:MM:SS.fffffffff`.
fn format_timestamp(nanos: i128) -> String {
    let secs = (nanos.div_euclid(1_000_000_000)) as i64;
    let sub = (nanos.rem_euclid(1_000_000_000)) as u32;
    match chrono::DateTime::from_timestamp(secs, sub) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
        None => "1970-01-01 00:00:00.000000000".to_string(),
    }
}

fn scalar(v: &str) -> Value {
    json!({ "ScalarValue": v })
}

fn null_datum() -> Value {
    json!({ "NullValue": true })
}

/// Execute a [`Plan`] against a table's ingested points, producing the
/// AWS-shaped `ColumnInfo` + `Row` lists.
pub fn execute(plan: &Plan, records: &[StoredRecord]) -> QueryResult {
    // Apply the time filter.
    let mut filtered: Vec<&StoredRecord> = records
        .iter()
        .filter(|r| match plan.time_filter {
            None => true,
            Some((op, threshold)) => match op {
                CmpOp::Gt => r.time_nanos > threshold,
                CmpOp::Ge => r.time_nanos >= threshold,
                CmpOp::Lt => r.time_nanos < threshold,
                CmpOp::Le => r.time_nanos <= threshold,
                CmpOp::Eq => r.time_nanos == threshold,
            },
        })
        .collect();

    // ORDER BY time.
    if let Some(desc) = plan.order_desc {
        filtered.sort_by(|a, b| {
            if desc {
                b.time_nanos.cmp(&a.time_nanos)
            } else {
                a.time_nanos.cmp(&b.time_nanos)
            }
        });
    }

    if let Projection::CountStar = plan.projection {
        let count = plan
            .limit
            .map(|l| filtered.len().min(l))
            .unwrap_or(filtered.len());
        return QueryResult {
            columns: vec![json!({ "Name": "_col0", "Type": { "ScalarType": "BIGINT" } })],
            rows: vec![json!({ "Data": [scalar(&count.to_string())] })],
        };
    }

    // SELECT *: derive the dimension columns and the measure value type columns
    // present across the (filtered) result set, deterministically ordered.
    if let Some(limit) = plan.limit {
        filtered.truncate(limit);
    }

    let mut dim_names: Vec<String> = Vec::new();
    let mut measure_types: Vec<String> = Vec::new();
    for r in &filtered {
        for (name, _) in &r.dimensions {
            if !dim_names.iter().any(|d| d == name) {
                dim_names.push(name.clone());
            }
        }
        if !measure_types.iter().any(|t| t == &r.measure_value_type) {
            measure_types.push(r.measure_value_type.clone());
        }
    }
    dim_names.sort();
    measure_types.sort();

    let mut columns: Vec<Value> = Vec::new();
    for d in &dim_names {
        columns.push(json!({ "Name": d, "Type": { "ScalarType": "VARCHAR" } }));
    }
    columns.push(json!({ "Name": "measure_name", "Type": { "ScalarType": "VARCHAR" } }));
    for t in &measure_types {
        columns.push(json!({
            "Name": format!("measure_value::{}", t.to_lowercase()),
            "Type": { "ScalarType": scalar_type_for_measure(t) }
        }));
    }
    columns.push(json!({ "Name": "time", "Type": { "ScalarType": "TIMESTAMP" } }));

    let mut rows: Vec<Value> = Vec::with_capacity(filtered.len());
    for r in &filtered {
        let mut data: Vec<Value> = Vec::with_capacity(columns.len());
        for d in &dim_names {
            match r.dimensions.iter().find(|(n, _)| n == d) {
                Some((_, v)) => data.push(scalar(v)),
                None => data.push(null_datum()),
            }
        }
        data.push(scalar(&r.measure_name));
        for t in &measure_types {
            if &r.measure_value_type == t {
                data.push(scalar(&r.measure_value));
            } else {
                data.push(null_datum());
            }
        }
        data.push(scalar(&format_timestamp(r.time_nanos)));
        rows.push(json!({ "Data": data }));
    }

    QueryResult { columns, rows }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(dims: &[(&str, &str)], mn: &str, mv: &str, ty: &str, t: i128) -> StoredRecord {
        StoredRecord {
            time_nanos: t,
            dimensions: dims
                .iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect(),
            measure_name: mn.to_string(),
            measure_value: mv.to_string(),
            measure_value_type: ty.to_string(),
        }
    }

    #[test]
    fn parse_select_star() {
        let p = parse_query(r#"SELECT * FROM "metrics"."cpu""#).unwrap();
        assert_eq!(p.database, "metrics");
        assert_eq!(p.table, "cpu");
        assert_eq!(p.projection, Projection::Star);
    }

    #[test]
    fn parse_count_and_order_and_limit() {
        let p = parse_query(r#"SELECT COUNT(*) FROM "m"."t" ORDER BY time DESC LIMIT 10"#).unwrap();
        assert_eq!(p.projection, Projection::CountStar);
        assert_eq!(p.order_desc, Some(true));
        assert_eq!(p.limit, Some(10));
    }

    #[test]
    fn parse_where_ago() {
        let p = parse_query(r#"SELECT * FROM "m"."t" WHERE time > ago(1h)"#).unwrap();
        assert!(matches!(p.time_filter, Some((CmpOp::Gt, _))));
    }

    #[test]
    fn unsupported_projection_is_validation() {
        let e = parse_query(r#"SELECT avg(x) FROM "m"."t""#).unwrap_err();
        assert!(e.message().contains("Unsupported projection"));
    }

    #[test]
    fn unsupported_where_is_validation() {
        let e = parse_query(r#"SELECT * FROM "m"."t" WHERE host = 'a'"#).unwrap_err();
        assert!(e.message().contains("Unsupported WHERE"));
    }

    #[test]
    fn execute_star_projects_dimension_measure_time() {
        let recs = vec![
            rec(&[("host", "a")], "cpu", "1.5", "DOUBLE", 1_000_000_000),
            rec(&[("host", "b")], "cpu", "2.5", "DOUBLE", 2_000_000_000),
        ];
        let p = parse_query(r#"SELECT * FROM "m"."t" ORDER BY time ASC"#).unwrap();
        let out = execute(&p, &recs);
        let col_names: Vec<&str> = out
            .columns
            .iter()
            .map(|c| c["Name"].as_str().unwrap())
            .collect();
        assert_eq!(
            col_names,
            vec!["host", "measure_name", "measure_value::double", "time"]
        );
        assert_eq!(out.rows.len(), 2);
        assert_eq!(out.rows[0]["Data"][0]["ScalarValue"], "a");
    }

    #[test]
    fn execute_count() {
        let recs = vec![
            rec(&[("host", "a")], "cpu", "1", "DOUBLE", 1),
            rec(&[("host", "b")], "cpu", "2", "DOUBLE", 2),
        ];
        let p = parse_query(r#"SELECT COUNT(*) FROM "m"."t""#).unwrap();
        let out = execute(&p, &recs);
        assert_eq!(out.rows.len(), 1);
        assert_eq!(out.rows[0]["Data"][0]["ScalarValue"], "2");
    }

    #[test]
    fn execute_limit_truncates() {
        let recs = vec![
            rec(&[("host", "a")], "cpu", "1", "DOUBLE", 1),
            rec(&[("host", "b")], "cpu", "2", "DOUBLE", 2),
            rec(&[("host", "c")], "cpu", "3", "DOUBLE", 3),
        ];
        let p = parse_query(r#"SELECT * FROM "m"."t" ORDER BY time ASC LIMIT 2"#).unwrap();
        let out = execute(&p, &recs);
        assert_eq!(out.rows.len(), 2);
    }
}
