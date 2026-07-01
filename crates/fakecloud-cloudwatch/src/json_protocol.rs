//! awsJson1_0 response support for CloudWatch.
//!
//! CloudWatch's Smithy model advertises `awsJson1_0` (target service shape
//! `GraniteServiceVersion20100801`) alongside the legacy `awsQuery` protocol.
//! Requests arriving over the JSON protocol are flattened into the awsQuery
//! flat-key param map by the central dispatcher, so every handler runs
//! unchanged and produces its usual awsQuery XML response.
//!
//! This module converts that XML response into the awsJson body a JSON client
//! expects. The transform strips the `<{Action}Response>` / `<{Action}Result>`
//! envelope and `ResponseMetadata`, turns awsQuery `<member>` lists into JSON
//! arrays and `<entry><key>/<value>` maps into JSON objects, and types leaf
//! values (numbers, booleans, epoch-second timestamps) so strict SDK
//! deserializers accept the result.

use fakecloud_core::service::{AwsResponse, ResponseBody};
use serde_json::{Map, Value};

use quick_xml::events::Event;
use quick_xml::reader::Reader;

/// awsJson1_0 content type used by CloudWatch JSON responses.
const JSON_CONTENT_TYPE: &str = "application/x-amz-json-1.0";

/// Output tags whose leaf text is an integer or double.
const NUMERIC_TAGS: &[&str] = &[
    "Period",
    "EvaluationPeriods",
    "DatapointsToAlarm",
    "Threshold",
    "Duration",
    "Size",
    "ActionsSuppressorWaitPeriod",
    "ActionsSuppressorExtensionPeriod",
    "StorageResolution",
    "SampleCount",
    "Average",
    "Sum",
    "Minimum",
    "Maximum",
    "Values",
    "Counts",
    "ExtendedStatistics",
];

/// Output tags whose leaf text is a boolean.
const BOOL_TAGS: &[&str] = &[
    "ActionsEnabled",
    "IncludeLinkedAccountsMetrics",
    "ApplyOnTransformedLogs",
    "ReturnData",
    "PeriodicSpikes",
];

/// Output tags whose leaf text is an ISO-8601 timestamp that awsJson renders as
/// epoch seconds.
const TIMESTAMP_TAGS: &[&str] = &[
    "Timestamp",
    "Timestamps",
    "AlarmConfigurationUpdatedTimestamp",
    "StateUpdatedTimestamp",
    "StateTransitionedTimestamp",
    "LastModified",
    "LastUpdateDate",
    "LastUpdatedTimestamp",
    "CreationDate",
    "StartDate",
    "ExpireDate",
    "StartTime",
    "EndTime",
];

/// Rebuild an XML awsQuery response as an awsJson1_0 response, preserving the
/// original HTTP status.
pub(crate) fn xml_response_to_json(resp: AwsResponse) -> AwsResponse {
    let status = resp.status;
    let ResponseBody::Bytes(bytes) = resp.body else {
        // CloudWatch handlers never stream a file body; fall back untouched.
        return resp;
    };
    let value = xml_to_json(&bytes);
    let body = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
    AwsResponse {
        status,
        content_type: JSON_CONTENT_TYPE.to_string(),
        body: ResponseBody::Bytes(body.into()),
        headers: resp.headers,
    }
}

/// A minimal XML element tree.
#[derive(Debug, Default)]
struct El {
    name: String,
    text: String,
    children: Vec<El>,
}

/// Parse the awsQuery XML envelope and convert its `<{Action}Result>` body to a
/// JSON object. Returns an empty object when there is no result body (e.g.
/// metadata-only responses like `PutMetricData`).
fn xml_to_json(xml: &[u8]) -> Value {
    let Some(root) = parse_xml(xml) else {
        return Value::Object(Map::new());
    };
    // The result body lives in the `<{Action}Result>` child; `ResponseMetadata`
    // is dropped.
    let Some(result) = root.children.iter().find(|c| c.name.ends_with("Result")) else {
        return Value::Object(Map::new());
    };
    match convert(result, "") {
        Some(v @ Value::Object(_)) => v,
        _ => Value::Object(Map::new()),
    }
}

/// Build an [`El`] tree from XML text. Returns the single root element.
fn parse_xml(xml: &[u8]) -> Option<El> {
    let mut reader = Reader::from_reader(xml);
    reader.config_mut().trim_text(true);
    let mut stack: Vec<El> = Vec::new();
    let mut root: Option<El> = None;
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = local_name(e.name().as_ref());
                stack.push(El {
                    name,
                    ..Default::default()
                });
            }
            Ok(Event::Empty(e)) => {
                let name = local_name(e.name().as_ref());
                let el = El {
                    name,
                    ..Default::default()
                };
                match stack.last_mut() {
                    Some(parent) => parent.children.push(el),
                    None => root = Some(el),
                }
            }
            Ok(Event::Text(e)) => {
                if let Some(top) = stack.last_mut() {
                    if let Ok(text) = e.unescape() {
                        top.text.push_str(text.as_ref());
                    }
                }
            }
            Ok(Event::CData(e)) => {
                if let Some(top) = stack.last_mut() {
                    if let Ok(s) = std::str::from_utf8(e.as_ref()) {
                        top.text.push_str(s);
                    }
                }
            }
            Ok(Event::End(_)) => {
                if let Some(done) = stack.pop() {
                    match stack.last_mut() {
                        Some(parent) => parent.children.push(done),
                        None => root = Some(done),
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(_) => return None,
        }
        buf.clear();
    }
    root
}

/// Strip an XML namespace prefix (`ns:Tag` -> `Tag`).
fn local_name(raw: &[u8]) -> String {
    let s = String::from_utf8_lossy(raw);
    match s.rsplit_once(':') {
        Some((_, local)) => local.to_string(),
        None => s.into_owned(),
    }
}

/// Convert an element to JSON. `parent_tag` carries the enclosing list/map tag
/// so scalar `<member>` leaves can be typed against the field they belong to.
fn convert(el: &El, parent_tag: &str) -> Option<Value> {
    if el.children.is_empty() {
        let text = el.text.trim();
        if text.is_empty() {
            return None;
        }
        // A leaf's own tag drives typing, except unnamed `member`/`value`
        // leaves which inherit the enclosing container's tag.
        let typing_tag = if el.name == "member" || el.name == "value" {
            parent_tag
        } else {
            &el.name
        };
        return Some(type_leaf(typing_tag, text));
    }

    // awsQuery list: every child is `<member>`.
    if el.children.iter().all(|c| c.name == "member") {
        let arr: Vec<Value> = el
            .children
            .iter()
            .filter_map(|m| convert(m, &el.name))
            .collect();
        return Some(Value::Array(arr));
    }

    // awsQuery map: every child is `<entry>` with `<key>`/`<value>`.
    if el.children.iter().all(|c| c.name == "entry") {
        let mut obj = Map::new();
        for entry in &el.children {
            let key = entry
                .children
                .iter()
                .find(|c| c.name == "key")
                .map(|k| k.text.trim().to_string());
            let val = entry
                .children
                .iter()
                .find(|c| c.name == "value")
                .and_then(|v| convert(v, &el.name));
            if let (Some(k), Some(v)) = (key, val) {
                obj.insert(k, v);
            }
        }
        return Some(Value::Object(obj));
    }

    // Plain structure.
    let mut obj = Map::new();
    for child in &el.children {
        if let Some(v) = convert(child, &el.name) {
            obj.insert(child.name.clone(), v);
        }
    }
    Some(Value::Object(obj))
}

/// Type a leaf value per the CloudWatch output schema.
fn type_leaf(tag: &str, text: &str) -> Value {
    if TIMESTAMP_TAGS.contains(&tag) {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(text) {
            let secs = dt.timestamp_millis() as f64 / 1000.0;
            if let Some(n) = serde_json::Number::from_f64(secs) {
                return Value::Number(n);
            }
        }
        return Value::String(text.to_string());
    }
    if BOOL_TAGS.contains(&tag) {
        return match text {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            other => Value::String(other.to_string()),
        };
    }
    if NUMERIC_TAGS.contains(&tag) {
        if !text.contains('.') && !text.contains('e') && !text.contains('E') {
            if let Ok(i) = text.parse::<i64>() {
                return Value::Number(i.into());
            }
        }
        if let Ok(f) = text.parse::<f64>() {
            if let Some(n) = serde_json::Number::from_f64(f) {
                return Value::Number(n);
            }
        }
        return Value::String(text.to_string());
    }
    Value::String(text.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(action: &str, inner: &str) -> Value {
        let xml = fakecloud_core::query::query_response_xml(
            action,
            "http://monitoring.amazonaws.com/doc/2010-08-01/",
            inner,
            "req-1",
        );
        xml_to_json(xml.as_bytes())
    }

    #[test]
    fn metadata_only_becomes_empty_object() {
        let xml = fakecloud_core::query::query_metadata_only_xml(
            "PutMetricData",
            "http://monitoring.amazonaws.com/doc/2010-08-01/",
            "req-1",
        );
        assert_eq!(xml_to_json(xml.as_bytes()), serde_json::json!({}));
    }

    #[test]
    fn list_metrics_members_become_array() {
        let inner = "<Metrics><member><Namespace>AWS/EC2</Namespace>\
            <MetricName>CPUUtilization</MetricName>\
            <Dimensions><member><Name>InstanceId</Name><Value>i-1</Value></member></Dimensions>\
            </member></Metrics><NextToken>tok</NextToken>";
        let v = json("ListMetrics", inner);
        assert_eq!(v["Metrics"][0]["Namespace"], "AWS/EC2");
        assert_eq!(v["Metrics"][0]["MetricName"], "CPUUtilization");
        assert_eq!(v["Metrics"][0]["Dimensions"][0]["Name"], "InstanceId");
        assert_eq!(v["Metrics"][0]["Dimensions"][0]["Value"], "i-1");
        assert_eq!(v["NextToken"], "tok");
        assert!(v["Metrics"].is_array());
    }

    #[test]
    fn datapoints_are_typed() {
        let inner = "<Label>CPUUtilization</Label><Datapoints><member>\
            <Timestamp>2020-01-01T00:00:00.000Z</Timestamp>\
            <Average>42.5</Average><SampleCount>3</SampleCount><Unit>Percent</Unit>\
            </member></Datapoints>";
        let v = json("GetMetricStatistics", inner);
        assert_eq!(v["Label"], "CPUUtilization");
        assert_eq!(v["Datapoints"][0]["Average"], 42.5);
        assert_eq!(v["Datapoints"][0]["SampleCount"], 3);
        assert_eq!(v["Datapoints"][0]["Unit"], "Percent");
        // Epoch seconds for 2020-01-01T00:00:00Z.
        assert_eq!(v["Datapoints"][0]["Timestamp"], 1577836800.0);
    }

    #[test]
    fn alarm_flags_typed() {
        let inner = "<MetricAlarms><member><AlarmName>cpu</AlarmName>\
            <ActionsEnabled>true</ActionsEnabled><Threshold>80.0</Threshold>\
            <EvaluationPeriods>2</EvaluationPeriods></member></MetricAlarms>";
        let v = json("DescribeAlarms", inner);
        assert_eq!(v["MetricAlarms"][0]["AlarmName"], "cpu");
        assert_eq!(v["MetricAlarms"][0]["ActionsEnabled"], true);
        assert_eq!(v["MetricAlarms"][0]["Threshold"], 80.0);
        assert_eq!(v["MetricAlarms"][0]["EvaluationPeriods"], 2);
    }

    #[test]
    fn timestamps_member_list_typed() {
        let inner = "<MetricDataResults><member><Id>m1</Id>\
            <Timestamps><member>2020-01-01T00:00:00.000Z</member></Timestamps>\
            <Values><member>1.5</member></Values></member></MetricDataResults>";
        let v = json("GetMetricData", inner);
        assert_eq!(v["MetricDataResults"][0]["Timestamps"][0], 1577836800.0);
        assert_eq!(v["MetricDataResults"][0]["Values"][0], 1.5);
    }
}
