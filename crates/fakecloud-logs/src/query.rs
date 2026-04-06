/// CloudWatch Logs Insights query language parser and executor.
///
/// Supports a subset of CWLI syntax:
/// - `fields @timestamp, @message` — select specific fields
/// - `filter @message like /pattern/` — filter by regex/substring
/// - `filter field = "value"` — filter by field equality
/// - `sort @timestamp desc` — sort results
/// - `limit N` — limit number of results
use crate::state::LogEvent;
use serde_json::{json, Value};

/// Parsed representation of a CWLI query.
#[derive(Debug, Default)]
pub struct ParsedQuery {
    pub fields: Vec<String>,
    pub filters: Vec<FilterClause>,
    pub sort_field: Option<String>,
    pub sort_desc: bool,
    pub limit: Option<usize>,
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

/// Parse a CWLI query string into a structured representation.
pub fn parse_query(query: &str) -> ParsedQuery {
    let mut parsed = ParsedQuery::default();

    // Split on pipe delimiter, trimming whitespace
    let commands: Vec<&str> = query.split('|').map(|s| s.trim()).collect();

    for cmd in commands {
        if cmd.is_empty() {
            continue;
        }

        if let Some(rest) = cmd
            .strip_prefix("fields ")
            .or_else(|| cmd.strip_prefix("fields\t"))
        {
            parsed.fields = rest
                .split(',')
                .map(|f| f.trim().to_string())
                .filter(|f| !f.is_empty())
                .collect();
        } else if let Some(rest) = cmd
            .strip_prefix("filter ")
            .or_else(|| cmd.strip_prefix("filter\t"))
        {
            if let Some(clause) = parse_filter_clause(rest.trim()) {
                parsed.filters.push(clause);
            }
        } else if let Some(rest) = cmd
            .strip_prefix("sort ")
            .or_else(|| cmd.strip_prefix("sort\t"))
        {
            let parts: Vec<&str> = rest.split_whitespace().collect();
            if !parts.is_empty() {
                parsed.sort_field = Some(parts[0].to_string());
                parsed.sort_desc =
                    parts.get(1).map(|s| s.eq_ignore_ascii_case("desc")) == Some(true);
            }
        } else if let Some(rest) = cmd
            .strip_prefix("limit ")
            .or_else(|| cmd.strip_prefix("limit\t"))
        {
            if let Ok(n) = rest.trim().parse::<usize>() {
                parsed.limit = Some(n);
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
            // Regex pattern - extract content between slashes
            pattern_str[1..pattern_str.len() - 1].to_string()
        } else {
            // Quoted string
            unquote(pattern_str)
        };
        return Some(FilterClause::Like { field, pattern });
    }

    // Try: field != "value"
    if let Some(ne_pos) = s.find(" != ") {
        let field = s[..ne_pos].trim().to_string();
        let value = unquote(s[ne_pos + 4..].trim());
        return Some(FilterClause::NotEquals { field, value });
    }

    // Try: field = "value"
    if let Some(eq_pos) = s.find(" = ") {
        let field = s[..eq_pos].trim().to_string();
        let value = unquote(s[eq_pos + 3..].trim());
        return Some(FilterClause::Equals { field, value });
    }

    None
}

fn unquote(s: &str) -> String {
    if (s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Get the value of a virtual field for a log event.
fn get_field_value(event: &LogEvent, field: &str, stream_name: &str) -> Option<String> {
    match field {
        "@timestamp" => {
            // Format as ISO 8601
            let secs = event.timestamp / 1000;
            let nsecs = ((event.timestamp % 1000) * 1_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                Some(dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
            } else {
                Some(event.timestamp.to_string())
            }
        }
        "@message" => Some(event.message.clone()),
        "@logStream" => Some(stream_name.to_string()),
        "@ingestionTime" => Some(event.ingestion_time.to_string()),
        "@ptr" => Some(format!("{}/{}", stream_name, event.timestamp)),
        _ => {
            // Try to extract from JSON message
            if let Ok(parsed) = serde_json::from_str::<Value>(&event.message) {
                // Strip leading @ if present for JSON field lookup
                let key = field.strip_prefix('@').unwrap_or(field);
                parsed.get(key).map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
            } else {
                None
            }
        }
    }
}

/// Check if a substring pattern matches a string (simple glob-like matching).
fn matches_pattern(haystack: &str, pattern: &str) -> bool {
    // Simple substring/regex-like matching without the regex crate.
    // For `/pattern/` syntax we do substring match.
    // For more complex patterns, we handle common regex anchors:
    //   ^..$ for exact match, ^ for starts-with, $ for ends-with
    if let Some(inner) = pattern.strip_prefix('^').and_then(|p| p.strip_suffix('$')) {
        haystack == inner
    } else if let Some(prefix) = pattern.strip_prefix('^') {
        haystack.starts_with(prefix)
    } else if let Some(suffix) = pattern.strip_suffix('$') {
        haystack.ends_with(suffix)
    } else {
        // Default: substring match
        haystack.contains(pattern)
    }
}

/// Apply a filter clause to an event, returning true if the event matches.
fn event_matches_filter(event: &LogEvent, stream_name: &str, clause: &FilterClause) -> bool {
    match clause {
        FilterClause::Equals { field, value } => get_field_value(event, field, stream_name)
            .map(|v| v == *value)
            .unwrap_or(false),
        FilterClause::NotEquals { field, value } => get_field_value(event, field, stream_name)
            .map(|v| v != *value)
            .unwrap_or(true),
        FilterClause::Like { field, pattern } => get_field_value(event, field, stream_name)
            .map(|v| matches_pattern(&v, pattern))
            .unwrap_or(false),
    }
}

/// A log event together with its stream name context, used during query execution.
struct EventWithContext<'a> {
    event: &'a LogEvent,
    stream_name: &'a str,
}

/// Execute a parsed query against a set of log events.
/// Returns results in the CloudWatch Logs Insights format: array of arrays of {field, value} objects.
pub fn execute_query(
    query: &ParsedQuery,
    events: &[(String, Vec<LogEvent>)], // (stream_name, events) pairs
    start_time_secs: i64,
    end_time_secs: i64,
) -> Vec<Value> {
    // Collect all events with context
    let mut all_events: Vec<EventWithContext> = Vec::new();
    for (stream_name, stream_events) in events {
        for event in stream_events {
            let event_time_secs = event.timestamp / 1000;
            if event_time_secs >= start_time_secs && event_time_secs < end_time_secs {
                all_events.push(EventWithContext { event, stream_name });
            }
        }
    }

    // Apply filters
    let filtered: Vec<&EventWithContext> = all_events
        .iter()
        .filter(|ec| {
            query
                .filters
                .iter()
                .all(|f| event_matches_filter(ec.event, ec.stream_name, f))
        })
        .collect();

    // Sort
    let mut sorted: Vec<&EventWithContext> = filtered;
    if let Some(ref sort_field) = query.sort_field {
        let field = sort_field.clone();
        let desc = query.sort_desc;
        sorted.sort_by(|a, b| {
            let va = get_field_value(a.event, &field, a.stream_name).unwrap_or_default();
            let vb = get_field_value(b.event, &field, b.stream_name).unwrap_or_default();
            if desc {
                vb.cmp(&va)
            } else {
                va.cmp(&vb)
            }
        });
    } else {
        // Default: sort by timestamp ascending
        sorted.sort_by_key(|ec| ec.event.timestamp);
    }

    // Apply limit
    if let Some(limit) = query.limit {
        sorted.truncate(limit);
    }

    // Determine which fields to output
    let output_fields = if query.fields.is_empty() {
        vec![
            "@timestamp".to_string(),
            "@message".to_string(),
            "@ptr".to_string(),
        ]
    } else {
        let mut fields = query.fields.clone();
        // Always include @ptr
        if !fields.iter().any(|f| f == "@ptr") {
            fields.push("@ptr".to_string());
        }
        fields
    };

    // Build result rows
    sorted
        .iter()
        .map(|ec| {
            let row: Vec<Value> = output_fields
                .iter()
                .filter_map(|field| {
                    get_field_value(ec.event, field, ec.stream_name)
                        .map(|value| json!({"field": field, "value": value}))
                })
                .collect();
            Value::Array(row)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_fields_and_limit() {
        let q = parse_query("fields @timestamp, @message | limit 5");
        assert_eq!(q.fields, vec!["@timestamp", "@message"]);
        assert_eq!(q.limit, Some(5));
    }

    #[test]
    fn parse_filter_equals() {
        let q = parse_query("filter level = \"ERROR\"");
        assert_eq!(q.filters.len(), 1);
        match &q.filters[0] {
            FilterClause::Equals { field, value } => {
                assert_eq!(field, "level");
                assert_eq!(value, "ERROR");
            }
            _ => panic!("expected Equals"),
        }
    }

    #[test]
    fn parse_filter_like_regex() {
        let q = parse_query("filter @message like /ERROR/");
        assert_eq!(q.filters.len(), 1);
        match &q.filters[0] {
            FilterClause::Like { field, pattern } => {
                assert_eq!(field, "@message");
                assert_eq!(pattern, "ERROR");
            }
            _ => panic!("expected Like"),
        }
    }

    #[test]
    fn parse_sort_desc() {
        let q = parse_query("sort @timestamp desc");
        assert_eq!(q.sort_field.as_deref(), Some("@timestamp"));
        assert!(q.sort_desc);
    }

    #[test]
    fn parse_sort_asc() {
        let q = parse_query("sort @timestamp asc");
        assert_eq!(q.sort_field.as_deref(), Some("@timestamp"));
        assert!(!q.sort_desc);
    }

    #[test]
    fn parse_complex_query() {
        let q = parse_query(
            "fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc | limit 10",
        );
        assert_eq!(q.fields, vec!["@timestamp", "@message"]);
        assert_eq!(q.filters.len(), 1);
        assert_eq!(q.sort_field.as_deref(), Some("@timestamp"));
        assert!(q.sort_desc);
        assert_eq!(q.limit, Some(10));
    }

    #[test]
    fn execute_query_filters_events() {
        let events = vec![(
            "stream-1".to_string(),
            vec![
                LogEvent {
                    timestamp: 1000000,
                    message: "ERROR: something broke".to_string(),
                    ingestion_time: 1000000,
                },
                LogEvent {
                    timestamp: 2000000,
                    message: "INFO: all good".to_string(),
                    ingestion_time: 2000000,
                },
                LogEvent {
                    timestamp: 3000000,
                    message: "ERROR: another failure".to_string(),
                    ingestion_time: 3000000,
                },
            ],
        )];

        let query = parse_query("filter @message like /ERROR/ | limit 10");
        let results = execute_query(&query, &events, 0, 10000);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn execute_query_limit() {
        let events = vec![(
            "stream-1".to_string(),
            vec![
                LogEvent {
                    timestamp: 1000000,
                    message: "msg1".to_string(),
                    ingestion_time: 1000000,
                },
                LogEvent {
                    timestamp: 2000000,
                    message: "msg2".to_string(),
                    ingestion_time: 2000000,
                },
                LogEvent {
                    timestamp: 3000000,
                    message: "msg3".to_string(),
                    ingestion_time: 3000000,
                },
            ],
        )];

        let query = parse_query("limit 2");
        let results = execute_query(&query, &events, 0, 10000);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn execute_query_fields_selection() {
        let events = vec![(
            "stream-1".to_string(),
            vec![LogEvent {
                timestamp: 1000000,
                message: "hello".to_string(),
                ingestion_time: 1000000,
            }],
        )];

        let query = parse_query("fields @message");
        let results = execute_query(&query, &events, 0, 10000);
        assert_eq!(results.len(), 1);

        let row = results[0].as_array().unwrap();
        let field_names: Vec<&str> = row.iter().map(|f| f["field"].as_str().unwrap()).collect();
        assert!(field_names.contains(&"@message"));
        assert!(field_names.contains(&"@ptr")); // always included
        assert!(!field_names.contains(&"@timestamp")); // not requested
    }

    #[test]
    fn execute_query_sort_desc() {
        let events = vec![(
            "stream-1".to_string(),
            vec![
                LogEvent {
                    timestamp: 1000000,
                    message: "first".to_string(),
                    ingestion_time: 1000000,
                },
                LogEvent {
                    timestamp: 3000000,
                    message: "third".to_string(),
                    ingestion_time: 3000000,
                },
                LogEvent {
                    timestamp: 2000000,
                    message: "second".to_string(),
                    ingestion_time: 2000000,
                },
            ],
        )];

        let query = parse_query("sort @timestamp desc");
        let results = execute_query(&query, &events, 0, 10000);
        assert_eq!(results.len(), 3);
        // First result should have the latest timestamp
        let first_msg = results[0]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["field"].as_str() == Some("@message"))
            .unwrap();
        assert_eq!(first_msg["value"].as_str().unwrap(), "third");
    }

    #[test]
    fn execute_query_json_field_filter() {
        let events = vec![(
            "stream-1".to_string(),
            vec![
                LogEvent {
                    timestamp: 1000000,
                    message: r#"{"level":"ERROR","msg":"fail"}"#.to_string(),
                    ingestion_time: 1000000,
                },
                LogEvent {
                    timestamp: 2000000,
                    message: r#"{"level":"INFO","msg":"ok"}"#.to_string(),
                    ingestion_time: 2000000,
                },
            ],
        )];

        let query = parse_query(r#"filter level = "ERROR""#);
        let results = execute_query(&query, &events, 0, 10000);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn execute_query_not_equals_filter() {
        let events = vec![(
            "stream-1".to_string(),
            vec![
                LogEvent {
                    timestamp: 1000000,
                    message: r#"{"level":"ERROR","msg":"fail"}"#.to_string(),
                    ingestion_time: 1000000,
                },
                LogEvent {
                    timestamp: 2000000,
                    message: r#"{"level":"INFO","msg":"ok"}"#.to_string(),
                    ingestion_time: 2000000,
                },
            ],
        )];

        let query = parse_query(r#"filter level != "ERROR""#);
        let results = execute_query(&query, &events, 0, 10000);
        assert_eq!(results.len(), 1);
        let msg = results[0]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["field"].as_str() == Some("@message"))
            .unwrap();
        assert!(msg["value"].as_str().unwrap().contains("INFO"));
    }
}
