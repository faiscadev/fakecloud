//! CloudWatch Logs Insights query language parser and executor.
//!
//! Supports the common subset of CWLI syntax as an ordered pipeline:
//! - `fields @timestamp, @message` / `display ...` — select output fields
//! - `filter field = "value"` / `!= ` / `like /pattern/` — filter rows
//! - `parse @message "* [*] *" as a, b, c` — extract fields via a glob
//! - `stats count(*) [as alias] by field, ...` — aggregate (count/sum/avg/min/
//!   max/count_distinct), optionally grouped
//! - `dedup field, ...` — drop duplicate rows by the given fields
//! - `sort @timestamp desc` — order rows
//! - `limit N` — cap row count
use crate::state::LogEvent;
use serde_json::{json, Value};

/// A parsed CWLI query: an ordered pipeline of commands.
#[derive(Debug, Default)]
pub struct ParsedQuery {
    pub commands: Vec<Command>,
}

#[derive(Debug)]
pub enum Command {
    Fields(Vec<String>),
    Display(Vec<String>),
    Filter(FilterClause),
    Sort { field: String, desc: bool },
    Limit(usize),
    Parse(ParseSpec),
    Dedup(Vec<String>),
    Stats { aggs: Vec<AggExpr>, by: Vec<String> },
}

#[derive(Debug)]
pub enum FilterClause {
    /// `filter field = "value"`
    Equals { field: String, value: String },
    /// `filter field != "value"`
    NotEquals { field: String, value: String },
    /// `filter field like /pattern/` or `filter field like "substring"`
    Like { field: String, pattern: String },
}

/// A `parse` directive: glob `pattern` applied to `source`, binding each `*`
/// wildcard (in order) to the corresponding name in `names`.
#[derive(Debug)]
pub struct ParseSpec {
    pub source: String,
    pub pattern: String,
    pub names: Vec<String>,
}

#[derive(Debug)]
pub struct AggExpr {
    pub func: AggFunc,
    pub field: Option<String>,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
}

/// Strip a leading keyword (followed by whitespace) from a command, returning
/// the remainder.
fn strip_keyword<'a>(cmd: &'a str, kw: &str) -> Option<&'a str> {
    let rest = cmd.strip_prefix(kw)?;
    // The keyword must be followed by whitespace (so `fieldspec` isn't matched
    // as `fields`).
    if rest.starts_with(|c: char| c.is_whitespace()) {
        Some(rest.trim_start())
    } else {
        None
    }
}

fn parse_field_list(s: &str) -> Vec<String> {
    s.split(',')
        .map(|f| f.trim().to_string())
        .filter(|f| !f.is_empty())
        .collect()
}

/// Parse a CWLI query string into a structured pipeline.
pub fn parse_query(query: &str) -> ParsedQuery {
    let mut parsed = ParsedQuery::default();

    for raw in query.split('|') {
        let cmd = raw.trim();
        if cmd.is_empty() {
            continue;
        }

        if let Some(rest) = strip_keyword(cmd, "fields") {
            parsed
                .commands
                .push(Command::Fields(parse_field_list(rest)));
        } else if let Some(rest) = strip_keyword(cmd, "display") {
            parsed
                .commands
                .push(Command::Display(parse_field_list(rest)));
        } else if let Some(rest) = strip_keyword(cmd, "filter") {
            if let Some(clause) = parse_filter_clause(rest.trim()) {
                parsed.commands.push(Command::Filter(clause));
            }
        } else if let Some(rest) = strip_keyword(cmd, "stats") {
            parsed.commands.push(parse_stats(rest));
        } else if let Some(rest) = strip_keyword(cmd, "parse") {
            if let Some(spec) = parse_parse(rest) {
                parsed.commands.push(Command::Parse(spec));
            }
        } else if let Some(rest) = strip_keyword(cmd, "dedup") {
            parsed.commands.push(Command::Dedup(parse_field_list(rest)));
        } else if let Some(rest) = strip_keyword(cmd, "sort") {
            let parts: Vec<&str> = rest.split_whitespace().collect();
            if !parts.is_empty() {
                parsed.commands.push(Command::Sort {
                    field: parts[0].to_string(),
                    desc: parts.get(1).map(|s| s.eq_ignore_ascii_case("desc")) == Some(true),
                });
            }
        } else if let Some(rest) = strip_keyword(cmd, "limit") {
            if let Ok(n) = rest.trim().parse::<usize>() {
                parsed.commands.push(Command::Limit(n));
            }
        }
    }

    parsed
}

fn parse_filter_clause(s: &str) -> Option<FilterClause> {
    // Try: field like /pattern/ or field like "substring"
    if let Some(like_pos) = s.find(" like ") {
        let field = s[..like_pos].trim().to_string();
        let pattern_str = s[like_pos + 6..].trim();
        let pattern = if pattern_str.starts_with('/') && pattern_str.ends_with('/') {
            pattern_str[1..pattern_str.len() - 1].to_string()
        } else {
            unquote(pattern_str)
        };
        return Some(FilterClause::Like { field, pattern });
    }

    if let Some(ne_pos) = s.find(" != ") {
        let field = s[..ne_pos].trim().to_string();
        let value = unquote(s[ne_pos + 4..].trim());
        return Some(FilterClause::NotEquals { field, value });
    }

    if let Some(eq_pos) = s.find(" = ") {
        let field = s[..eq_pos].trim().to_string();
        let value = unquote(s[eq_pos + 3..].trim());
        return Some(FilterClause::Equals { field, value });
    }

    None
}

/// Parse a `stats` command body (the text after `stats`).
fn parse_stats(rest: &str) -> Command {
    // Split off the optional `by <fields>` clause (case-insensitive).
    let lower = rest.to_ascii_lowercase();
    let (agg_part, by_part) = match lower.find(" by ") {
        Some(pos) => (&rest[..pos], Some(rest[pos + 4..].trim())),
        None => (rest, None),
    };
    let aggs = split_top_level_commas(agg_part)
        .iter()
        .filter_map(|s| parse_agg_expr(s.trim()))
        .collect();
    let by = by_part.map(parse_field_list).unwrap_or_default();
    Command::Stats { aggs, by }
}

/// Split on commas that are not nested inside parentheses, so
/// `count(*), avg(latency)` splits but `pct(latency, 99)` doesn't.
fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut current = String::new();
    for c in s.chars() {
        match c {
            '(' => {
                depth += 1;
                current.push(c);
            }
            ')' => {
                depth -= 1;
                current.push(c);
            }
            ',' if depth == 0 => {
                out.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

fn parse_agg_expr(s: &str) -> Option<AggExpr> {
    // Split off an optional `as alias`.
    let (expr, alias) = match s.to_ascii_lowercase().find(" as ") {
        Some(pos) => (s[..pos].trim(), Some(s[pos + 4..].trim().to_string())),
        None => (s.trim(), None),
    };
    let open = expr.find('(')?;
    let close = expr.rfind(')')?;
    if close < open {
        return None;
    }
    let func_name = expr[..open].trim().to_ascii_lowercase();
    let arg = expr[open + 1..close].trim();
    let func = match func_name.as_str() {
        "count" => AggFunc::Count,
        "sum" => AggFunc::Sum,
        "avg" | "average" => AggFunc::Avg,
        "min" => AggFunc::Min,
        "max" => AggFunc::Max,
        "count_distinct" | "countdistinct" => AggFunc::CountDistinct,
        _ => return None,
    };
    let field = if arg.is_empty() || arg == "*" {
        None
    } else {
        Some(arg.to_string())
    };
    let alias = alias.unwrap_or_else(|| expr.to_string());
    Some(AggExpr { func, field, alias })
}

/// Parse a `parse <source> "<glob>" as a, b, ...` command body.
fn parse_parse(rest: &str) -> Option<ParseSpec> {
    let rest = rest.trim();
    // The source token is everything up to the first quote.
    let quote_pos = rest.find(['"', '\''])?;
    let source = rest[..quote_pos].trim().to_string();
    if source.is_empty() {
        return None;
    }
    let quote = rest.as_bytes()[quote_pos] as char;
    let after = &rest[quote_pos + 1..];
    let end = after.find(quote)?;
    let pattern = after[..end].to_string();
    let tail = after[end + 1..].trim();
    let names = match tail.to_ascii_lowercase().strip_prefix("as ") {
        Some(_) => parse_field_list(&tail[3..]),
        None => Vec::new(),
    };
    Some(ParseSpec {
        source,
        pattern,
        names,
    })
}

fn unquote(s: &str) -> String {
    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// A materialized query row: ordered (field, value) pairs. Insertion order is
/// preserved so output column ordering is stable.
#[derive(Clone, Default)]
struct Record {
    fields: Vec<(String, String)>,
}

impl Record {
    fn get(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    fn set(&mut self, name: &str, value: String) {
        if let Some(entry) = self.fields.iter_mut().find(|(k, _)| k == name) {
            entry.1 = value;
        } else {
            self.fields.push((name.to_string(), value));
        }
    }
}

/// Format a timestamp (epoch millis) the way CWLI renders `@timestamp`.
fn format_timestamp(ms: i64) -> String {
    let secs = ms / 1000;
    let nsecs = ((ms % 1000) * 1_000_000) as u32;
    match chrono::DateTime::from_timestamp(secs, nsecs) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        None => ms.to_string(),
    }
}

/// Mint a GetLogRecord-compatible `@ptr`: base64(`<group>|<stream>|<index>`).
fn encode_ptr(group: &str, stream: &str, index: usize) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(format!("{group}|{stream}|{index}").as_bytes())
}

/// Build the base record for a log event, including the synthetic `@`-fields
/// and any top-level keys discovered from a JSON message body.
fn build_record(event: &LogEvent, group: &str, stream: &str, index: usize) -> Record {
    let mut r = Record::default();
    r.set("@timestamp", format_timestamp(event.timestamp));
    r.set("@message", event.message.clone());
    r.set("@logStream", stream.to_string());
    r.set("@ingestionTime", event.ingestion_time.to_string());
    r.set("@ptr", encode_ptr(group, stream, index));
    if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(&event.message) {
        for (k, v) in map {
            let value = match v {
                Value::String(s) => s,
                other => other.to_string(),
            };
            // Discovered JSON fields don't clobber the synthetic ones.
            if r.get(&k).is_none() {
                r.set(&k, value);
            }
        }
    }
    r
}

/// Resolve a field for filtering/grouping, allowing the `@`-stripped JSON
/// fallback (e.g. `level` resolves a JSON key `level`).
fn record_field(r: &Record, name: &str) -> Option<String> {
    if let Some(v) = r.get(name) {
        return Some(v.to_string());
    }
    let stripped = name.strip_prefix('@').unwrap_or(name);
    r.get(stripped).map(|v| v.to_string())
}

fn matches_pattern(haystack: &str, pattern: &str) -> bool {
    if let Some(inner) = pattern.strip_prefix('^').and_then(|p| p.strip_suffix('$')) {
        haystack == inner
    } else if let Some(prefix) = pattern.strip_prefix('^') {
        haystack.starts_with(prefix)
    } else if let Some(suffix) = pattern.strip_suffix('$') {
        haystack.ends_with(suffix)
    } else {
        haystack.contains(pattern)
    }
}

fn record_matches_filter(r: &Record, clause: &FilterClause) -> bool {
    match clause {
        FilterClause::Equals { field, value } => {
            record_field(r, field).map(|v| v == *value).unwrap_or(false)
        }
        FilterClause::NotEquals { field, value } => {
            record_field(r, field).map(|v| v != *value).unwrap_or(true)
        }
        FilterClause::Like { field, pattern } => record_field(r, field)
            .map(|v| matches_pattern(&v, pattern))
            .unwrap_or(false),
    }
}

/// Apply a glob `parse` to a record, binding each `*` to the matching name.
fn apply_parse(r: &mut Record, spec: &ParseSpec) {
    let Some(source) = record_field(r, &spec.source) else {
        return;
    };
    let segments: Vec<&str> = spec.pattern.split('*').collect();
    let mut captures: Vec<String> = Vec::new();
    let mut pos = 0usize;
    // The first segment is a required prefix.
    if let Some(first) = segments.first() {
        if !source[pos..].starts_with(first) {
            return;
        }
        pos += first.len();
    }
    for seg in segments.iter().skip(1) {
        if seg.is_empty() {
            // Trailing wildcard: capture the rest.
            captures.push(source[pos..].to_string());
            pos = source.len();
            continue;
        }
        let Some(found) = source[pos..].find(seg) else {
            return;
        };
        captures.push(source[pos..pos + found].to_string());
        pos += found + seg.len();
    }
    for (name, value) in spec.names.iter().zip(captures) {
        r.set(name, value);
    }
}

fn parse_number(s: &str) -> Option<f64> {
    s.trim().parse::<f64>().ok()
}

/// Format an aggregate result, dropping a trailing `.0` so counts read as
/// integers the way CloudWatch renders them.
fn format_number(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

fn compute_agg(agg: &AggExpr, group: &[&Record]) -> String {
    match agg.func {
        AggFunc::Count => match &agg.field {
            None => format_number(group.len() as f64),
            Some(f) => {
                let n = group
                    .iter()
                    .filter(|r| record_field(r, f).is_some())
                    .count();
                format_number(n as f64)
            }
        },
        AggFunc::CountDistinct => {
            let field = match &agg.field {
                Some(f) => f,
                None => return "0".to_string(),
            };
            let mut seen = std::collections::BTreeSet::new();
            for r in group {
                if let Some(v) = record_field(r, field) {
                    seen.insert(v);
                }
            }
            format_number(seen.len() as f64)
        }
        AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {
            let field = match &agg.field {
                Some(f) => f,
                None => return "0".to_string(),
            };
            let nums: Vec<f64> = group
                .iter()
                .filter_map(|r| record_field(r, field).and_then(|v| parse_number(&v)))
                .collect();
            if nums.is_empty() {
                return "0".to_string();
            }
            let v = match agg.func {
                AggFunc::Sum => nums.iter().sum(),
                AggFunc::Avg => nums.iter().sum::<f64>() / nums.len() as f64,
                AggFunc::Min => nums.iter().cloned().fold(f64::INFINITY, f64::min),
                AggFunc::Max => nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                _ => unreachable!(),
            };
            format_number(v)
        }
    }
}

/// One stream's events for query execution.
pub struct QueryStream {
    pub group_name: String,
    pub stream_name: String,
    /// Each event paired with its index in the stream's full event list so
    /// `@ptr` round-trips through GetLogRecord.
    pub events: Vec<(usize, LogEvent)>,
}

/// Execute a parsed query against a set of streams, returning results in the
/// CloudWatch Logs Insights format: an array of rows, each an array of
/// `{field, value}` objects.
pub fn execute_query(
    query: &ParsedQuery,
    streams: &[QueryStream],
    start_time_secs: i64,
    end_time_secs: i64,
) -> Vec<Value> {
    // Materialize records inside the time window.
    let mut records: Vec<Record> = Vec::new();
    for stream in streams {
        for (index, event) in &stream.events {
            let event_time_secs = event.timestamp / 1000;
            if event_time_secs >= start_time_secs && event_time_secs < end_time_secs {
                records.push(build_record(
                    event,
                    &stream.group_name,
                    &stream.stream_name,
                    *index,
                ));
            }
        }
    }

    // Default ordering is by timestamp ascending; an explicit `sort` overrides.
    records.sort_by(|a, b| {
        a.get("@timestamp")
            .unwrap_or_default()
            .cmp(b.get("@timestamp").unwrap_or_default())
    });

    let mut explicit_fields: Option<Vec<String>> = None;
    let mut aggregated = false;

    for cmd in &query.commands {
        match cmd {
            Command::Filter(clause) => {
                records.retain(|r| record_matches_filter(r, clause));
            }
            Command::Parse(spec) => {
                for r in records.iter_mut() {
                    apply_parse(r, spec);
                }
            }
            Command::Fields(f) | Command::Display(f) => {
                explicit_fields = Some(f.clone());
            }
            Command::Dedup(fields) => {
                let mut seen = std::collections::HashSet::new();
                records.retain(|r| {
                    let key: Vec<Option<String>> =
                        fields.iter().map(|f| record_field(r, f)).collect();
                    seen.insert(format!("{key:?}"))
                });
            }
            Command::Sort { field, desc } => {
                records.sort_by(|a, b| {
                    let va = record_field(a, field).unwrap_or_default();
                    let vb = record_field(b, field).unwrap_or_default();
                    if *desc {
                        vb.cmp(&va)
                    } else {
                        va.cmp(&vb)
                    }
                });
            }
            Command::Limit(n) => {
                records.truncate(*n);
            }
            Command::Stats { aggs, by } => {
                records = run_stats(&records, aggs, by);
                aggregated = true;
                // After aggregation, the output columns are the by-fields plus
                // the agg aliases.
                let mut cols: Vec<String> = by.clone();
                cols.extend(aggs.iter().map(|a| a.alias.clone()));
                explicit_fields = Some(cols);
            }
        }
    }

    // Decide output columns.
    let mut output_fields: Vec<String> = match explicit_fields {
        Some(f) => f,
        None => vec![
            "@timestamp".to_string(),
            "@message".to_string(),
            "@ptr".to_string(),
        ],
    };
    // Raw (non-aggregated) results always carry @ptr so callers can drill in.
    if !aggregated && !output_fields.iter().any(|f| f == "@ptr") {
        output_fields.push("@ptr".to_string());
    }

    records
        .iter()
        .map(|r| {
            let row: Vec<Value> = output_fields
                .iter()
                .filter_map(|field| {
                    record_field(r, field).map(|value| json!({"field": field, "value": value}))
                })
                .collect();
            Value::Array(row)
        })
        .collect()
}

/// Group records and compute the requested aggregates, returning one synthetic
/// record per group.
fn run_stats(records: &[Record], aggs: &[AggExpr], by: &[String]) -> Vec<Record> {
    use std::collections::BTreeMap;
    let mut groups: BTreeMap<Vec<String>, Vec<&Record>> = BTreeMap::new();
    for r in records {
        let key: Vec<String> = by
            .iter()
            .map(|f| record_field(r, f).unwrap_or_default())
            .collect();
        groups.entry(key).or_default().push(r);
    }

    let mut out = Vec::new();
    for (key, group) in groups {
        let mut rec = Record::default();
        for (field, value) in by.iter().zip(key.iter()) {
            rec.set(field, value.clone());
        }
        for agg in aggs {
            rec.set(&agg.alias, compute_agg(agg, &group));
        }
        out.push(rec);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(name: &str, events: Vec<LogEvent>) -> QueryStream {
        QueryStream {
            group_name: "/g".to_string(),
            stream_name: name.to_string(),
            events: events.into_iter().enumerate().collect(),
        }
    }

    fn ev(ts: i64, msg: &str) -> LogEvent {
        LogEvent {
            timestamp: ts,
            message: msg.to_string(),
            ingestion_time: ts,
        }
    }

    #[test]
    fn parse_fields_and_limit() {
        let q = parse_query("fields @timestamp, @message | limit 5");
        assert_eq!(q.commands.len(), 2);
        match &q.commands[0] {
            Command::Fields(f) => assert_eq!(f, &vec!["@timestamp", "@message"]),
            _ => panic!("expected Fields"),
        }
        match &q.commands[1] {
            Command::Limit(n) => assert_eq!(*n, 5),
            _ => panic!("expected Limit"),
        }
    }

    #[test]
    fn execute_filters_events() {
        let streams = vec![stream(
            "s1",
            vec![
                ev(1000000, "ERROR: broke"),
                ev(2000000, "INFO: ok"),
                ev(3000000, "ERROR: again"),
            ],
        )];
        let q = parse_query("filter @message like /ERROR/ | limit 10");
        let r = execute_query(&q, &streams, 0, 10000);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn execute_json_field_filter() {
        let streams = vec![stream(
            "s1",
            vec![
                ev(1000000, r#"{"level":"ERROR","msg":"fail"}"#),
                ev(2000000, r#"{"level":"INFO","msg":"ok"}"#),
            ],
        )];
        let q = parse_query(r#"filter level = "ERROR""#);
        let r = execute_query(&q, &streams, 0, 10000);
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn execute_not_equals_filter() {
        let streams = vec![stream(
            "s1",
            vec![
                ev(1000000, r#"{"level":"ERROR"}"#),
                ev(2000000, r#"{"level":"INFO"}"#),
            ],
        )];
        let q = parse_query(r#"filter level != "ERROR""#);
        let r = execute_query(&q, &streams, 0, 10000);
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn stats_count_by_field() {
        let streams = vec![stream(
            "s1",
            vec![
                ev(1000000, r#"{"level":"ERROR"}"#),
                ev(2000000, r#"{"level":"INFO"}"#),
                ev(3000000, r#"{"level":"ERROR"}"#),
                ev(4000000, r#"{"level":"ERROR"}"#),
            ],
        )];
        let q = parse_query("stats count(*) by level");
        let rows = execute_query(&q, &streams, 0, 10000);
        assert_eq!(rows.len(), 2);
        let error_row = rows
            .iter()
            .find(|row| {
                row.as_array()
                    .unwrap()
                    .iter()
                    .any(|f| f["field"] == "level" && f["value"] == "ERROR")
            })
            .unwrap();
        let count = error_row
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["field"] == "count(*)")
            .unwrap();
        assert_eq!(count["value"], "3");
    }

    #[test]
    fn stats_sum_avg() {
        let streams = vec![stream(
            "s1",
            vec![
                ev(1000000, r#"{"svc":"a","latency":10}"#),
                ev(2000000, r#"{"svc":"a","latency":30}"#),
            ],
        )];
        let q = parse_query("stats sum(latency) as total, avg(latency) as mean by svc");
        let rows = execute_query(&q, &streams, 0, 10000);
        assert_eq!(rows.len(), 1);
        let row = rows[0].as_array().unwrap();
        let total = row.iter().find(|f| f["field"] == "total").unwrap();
        let mean = row.iter().find(|f| f["field"] == "mean").unwrap();
        assert_eq!(total["value"], "40");
        assert_eq!(mean["value"], "20");
    }

    #[test]
    fn ptr_is_base64_group_stream_index() {
        use base64::Engine;
        let streams = vec![stream("s1", vec![ev(1000000, "hello")])];
        let q = parse_query("fields @message");
        let rows = execute_query(&q, &streams, 0, 10000);
        let row = rows[0].as_array().unwrap();
        let ptr = row.iter().find(|f| f["field"] == "@ptr").unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(ptr["value"].as_str().unwrap())
            .unwrap();
        assert_eq!(String::from_utf8(decoded).unwrap(), "/g|s1|0");
    }

    #[test]
    fn parse_glob_extracts_fields() {
        let streams = vec![stream("s1", vec![ev(1000000, "GET /api 200")])];
        let q =
            parse_query("parse @message \"* * *\" as method, path, code | filter code = \"200\"");
        let rows = execute_query(&q, &streams, 0, 10000);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn dedup_drops_duplicates() {
        let streams = vec![stream(
            "s1",
            vec![
                ev(1000000, r#"{"level":"ERROR"}"#),
                ev(2000000, r#"{"level":"ERROR"}"#),
                ev(3000000, r#"{"level":"INFO"}"#),
            ],
        )];
        let q = parse_query("dedup level");
        let rows = execute_query(&q, &streams, 0, 10000);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn sort_desc_by_timestamp() {
        let streams = vec![stream(
            "s1",
            vec![
                ev(1000000, "first"),
                ev(3000000, "third"),
                ev(2000000, "second"),
            ],
        )];
        let q = parse_query("sort @timestamp desc");
        let rows = execute_query(&q, &streams, 0, 10000);
        let first = rows[0].as_array().unwrap();
        let msg = first.iter().find(|f| f["field"] == "@message").unwrap();
        assert_eq!(msg["value"], "third");
    }
}
