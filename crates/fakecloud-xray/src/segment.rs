//! X-Ray trace segment document parsing.
//!
//! `PutTraceSegments` accepts a list of segment documents, each a JSON string in
//! the [X-Ray segment document
//! format](https://docs.aws.amazon.com/xray/latest/devguide/xray-api-segmentdocuments.html).
//! We parse the fields fakecloud actually models -- `trace_id`, `id`, `name`,
//! `start_time`, `end_time`, `parent_id`, `origin`, the `http` request/response,
//! the `error`/`fault`/`throttle` flags, and the nested `subsegments` (walking
//! `namespace: "remote"` subsegments as downstream service calls) -- into a
//! [`StoredSegment`] kept for the data-plane reads. The raw document string is
//! retained verbatim so `BatchGetTraces` echoes exactly what was ingested.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A downstream service call discovered inside a segment's subsegments (a
/// subsegment with `namespace: "remote"`). Drives the service-graph edges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Downstream {
    pub name: String,
    pub start_time: f64,
    pub end_time: Option<f64>,
    pub fault: bool,
    pub error: bool,
    pub throttle: bool,
    /// The subsegment's declared type, if any (e.g. `AWS::DynamoDB::Table`).
    pub kind: Option<String>,
}

/// A parsed, stored X-Ray segment. Retains the raw `document` for verbatim
/// re-emission and the extracted fields the data-plane reads compute over.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSegment {
    pub trace_id: String,
    pub id: String,
    pub name: String,
    pub start_time: f64,
    pub end_time: Option<f64>,
    pub parent_id: Option<String>,
    pub origin: Option<String>,
    pub http_status: Option<i64>,
    pub http_method: Option<String>,
    pub http_url: Option<String>,
    pub fault: bool,
    pub error: bool,
    pub throttle: bool,
    pub downstream: Vec<Downstream>,
    /// The raw segment document as ingested, echoed by `BatchGetTraces`.
    pub document: String,
}

impl StoredSegment {
    /// Duration in seconds (`end_time - start_time`), when the segment is
    /// complete (has an `end_time`). An in-progress segment has no duration.
    pub fn duration(&self) -> Option<f64> {
        self.end_time.map(|e| (e - self.start_time).max(0.0))
    }
}

/// Error parsing a single segment document, mapped to an
/// `UnprocessedTraceSegment` entry by the handler.
#[derive(Debug)]
pub struct ParseError {
    /// The segment `id`, when it could be read from the malformed document.
    pub id: Option<String>,
    pub code: &'static str,
    pub message: String,
}

fn bool_field(v: &Value, key: &str) -> bool {
    v.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn f64_field(v: &Value, key: &str) -> Option<f64> {
    v.get(key).and_then(Value::as_f64)
}

fn str_field(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string)
}

/// Recursively collect `namespace: "remote"` subsegments as downstream calls.
fn collect_downstream(seg: &Value, out: &mut Vec<Downstream>) {
    let Some(subs) = seg.get("subsegments").and_then(Value::as_array) else {
        return;
    };
    for sub in subs {
        let namespace = sub.get("namespace").and_then(Value::as_str);
        if namespace == Some("remote") {
            if let Some(name) = str_field(sub, "name") {
                out.push(Downstream {
                    name,
                    start_time: f64_field(sub, "start_time").unwrap_or(0.0),
                    end_time: f64_field(sub, "end_time"),
                    fault: bool_field(sub, "fault"),
                    error: bool_field(sub, "error"),
                    throttle: bool_field(sub, "throttle"),
                    kind: str_field(sub, "type"),
                });
            }
        }
        // Recurse: downstream calls can be nested inside local subsegments.
        collect_downstream(sub, out);
    }
}

/// Parse a single segment document string into a [`StoredSegment`].
///
/// A segment document must be a JSON object carrying at minimum a `trace_id`,
/// an `id`, and a `name` (per the X-Ray segment document schema); anything else
/// is rejected as a `ParseError` that surfaces as an `UnprocessedTraceSegment`.
pub fn parse_segment(document: &str) -> Result<StoredSegment, ParseError> {
    let v: Value = serde_json::from_str(document).map_err(|e| ParseError {
        id: None,
        code: "MalformedJson",
        message: format!("Segment document is not valid JSON: {e}"),
    })?;
    if !v.is_object() {
        return Err(ParseError {
            id: None,
            code: "MalformedJson",
            message: "Segment document must be a JSON object.".to_string(),
        });
    }
    let id = str_field(&v, "id");
    let trace_id = str_field(&v, "trace_id").ok_or_else(|| ParseError {
        id: id.clone(),
        code: "MissingTraceId",
        message: "Segment document is missing required field 'trace_id'.".to_string(),
    })?;
    let seg_id = id.clone().ok_or_else(|| ParseError {
        id: None,
        code: "MissingId",
        message: "Segment document is missing required field 'id'.".to_string(),
    })?;
    let name = str_field(&v, "name").ok_or_else(|| ParseError {
        id: id.clone(),
        code: "MissingName",
        message: "Segment document is missing required field 'name'.".to_string(),
    })?;
    let start_time = f64_field(&v, "start_time").ok_or_else(|| ParseError {
        id: id.clone(),
        code: "MissingStartTime",
        message: "Segment document is missing required field 'start_time'.".to_string(),
    })?;

    let http = v.get("http");
    let http_status = http
        .and_then(|h| h.get("response"))
        .and_then(|r| r.get("status"))
        .and_then(Value::as_i64);
    let http_method = http
        .and_then(|h| h.get("request"))
        .and_then(|r| r.get("method"))
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string);
    let http_url = http
        .and_then(|h| h.get("request"))
        .and_then(|r| r.get("url"))
        .and_then(Value::as_str)
        .map(std::string::ToString::to_string);

    let mut downstream = Vec::new();
    collect_downstream(&v, &mut downstream);

    Ok(StoredSegment {
        trace_id,
        id: seg_id,
        name,
        start_time,
        end_time: f64_field(&v, "end_time"),
        parent_id: str_field(&v, "parent_id"),
        origin: str_field(&v, "origin"),
        http_status,
        http_method,
        http_url,
        fault: bool_field(&v, "fault"),
        error: bool_field(&v, "error"),
        throttle: bool_field(&v, "throttle"),
        downstream,
        document: document.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_segment() {
        let doc = r#"{"trace_id":"1-58406520-a006649127e371903a2de979","id":"6b55dcb43fd8c8c1","name":"api","start_time":1.0,"end_time":2.0}"#;
        let seg = parse_segment(doc).unwrap();
        assert_eq!(seg.trace_id, "1-58406520-a006649127e371903a2de979");
        assert_eq!(seg.name, "api");
        assert_eq!(seg.duration(), Some(1.0));
        assert_eq!(seg.document, doc);
    }

    #[test]
    fn reads_http_and_fault_flags() {
        let doc = r#"{"trace_id":"1-x","id":"aaaa","name":"web","start_time":1.0,"end_time":2.5,
            "fault":true,"http":{"request":{"method":"GET","url":"http://x/"},"response":{"status":500}}}"#;
        let seg = parse_segment(doc).unwrap();
        assert!(seg.fault);
        assert_eq!(seg.http_status, Some(500));
        assert_eq!(seg.http_method.as_deref(), Some("GET"));
    }

    #[test]
    fn collects_remote_downstream_calls() {
        let doc = r#"{"trace_id":"1-x","id":"aaaa","name":"web","start_time":1.0,"end_time":3.0,
            "subsegments":[
              {"id":"bbbb","name":"local","start_time":1.1,"end_time":1.2,
               "subsegments":[{"id":"cccc","name":"dynamo","namespace":"remote","start_time":1.3,"end_time":1.9,"error":true,"type":"AWS::DynamoDB::Table"}]},
              {"id":"dddd","name":"backend","namespace":"remote","start_time":2.0,"end_time":2.8}
            ]}"#;
        let seg = parse_segment(doc).unwrap();
        assert_eq!(seg.downstream.len(), 2);
        let names: Vec<&str> = seg.downstream.iter().map(|d| d.name.as_str()).collect();
        assert!(names.contains(&"dynamo"));
        assert!(names.contains(&"backend"));
        let dynamo = seg.downstream.iter().find(|d| d.name == "dynamo").unwrap();
        assert!(dynamo.error);
        assert_eq!(dynamo.kind.as_deref(), Some("AWS::DynamoDB::Table"));
    }

    #[test]
    fn rejects_missing_trace_id() {
        let err = parse_segment(r#"{"id":"aaaa","name":"x","start_time":1.0}"#).unwrap_err();
        assert_eq!(err.code, "MissingTraceId");
    }

    #[test]
    fn rejects_malformed_json() {
        let err = parse_segment("not json").unwrap_err();
        assert_eq!(err.code, "MalformedJson");
    }
}
