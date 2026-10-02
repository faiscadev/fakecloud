//! awsJson1_0 and rpcv2Cbor response support for CloudWatch.
//!
//! CloudWatch's Smithy model advertises `rpcv2Cbor` and `awsJson1_0` (service
//! shape `GraniteServiceVersion20100801`) alongside the legacy `awsQuery`
//! protocol. Current aws-sdk-rust speaks rpcv2Cbor, aws-sdk-js-v3 and botocore
//! speak awsJson1_0. Requests arriving over either are flattened into the
//! awsQuery flat-key param map by the central dispatcher, so every handler
//! runs unchanged and produces its usual awsQuery XML response.
//!
//! This module converts that XML response into the body the caller's protocol
//! expects. The transform strips the `<{Action}Response>` / `<{Action}Result>`
//! envelope and `ResponseMetadata`, turns awsQuery `<member>` lists into
//! arrays and `<entry><key>/<value>` maps into maps, and types leaf values
//! against the output schema (integers, doubles, booleans, timestamps, blobs)
//! so strict SDK deserializers accept the result. The member-type tables below
//! are checked against `aws-models/cloudwatch.json` by a unit test.

use base64::Engine;
use fakecloud_core::cbor::{self, CborValue};
use fakecloud_core::service::{AwsResponse, ResponseBody};
use serde_json::{Map, Value};

use quick_xml::events::Event;
use quick_xml::reader::Reader;

/// awsJson1_0 content type used by CloudWatch JSON responses.
const JSON_CONTENT_TYPE: &str = "application/x-amz-json-1.0";

/// Output members whose leaf text is a Smithy `double` (for a list or map,
/// its elements / values).
const DOUBLE_TAGS: &[&str] = &[
    "AggregateValue",
    "ApproximateAggregateValue",
    "ApproximateValue",
    "Average",
    "ExtendedStatistics",
    "MaxContributorValue",
    "Maximum",
    "Minimum",
    "SampleCount",
    "Sum",
    "Threshold",
    "UniqueContributors",
    "Values",
];

/// Output members whose leaf text is a Smithy `integer` or `long`.
const INTEGER_TAGS: &[&str] = &[
    "ActionLogLineCount",
    "ActionsSuppressorExtensionPeriod",
    "ActionsSuppressorWaitPeriod",
    "ApproximateUniqueCount",
    "DatapointsToAlarm",
    "EndTimeOffset",
    "EvaluationInterval",
    "EvaluationPeriods",
    "PendingPeriod",
    "Period",
    "QueryResultsToAlarm",
    "QueryResultsToEvaluate",
    "RecoveryPeriod",
    "Size",
    "StartTimeOffset",
    "WarmUpPeriodDurationInMinutes",
];

/// Output members that are lists. When such a container is present but empty
/// (no `<member>` children), it must serialize as an empty array rather than
/// being omitted, so callers don't see a missing field where the XML
/// explicitly carried an empty list.
const LIST_TAGS: &[&str] = &[
    "AdditionalStatistics",
    "AlarmActions",
    "AlarmContributors",
    "AlarmHistoryItems",
    "AlarmMuteRuleSummaries",
    "AlarmNames",
    "AnomalyDetectors",
    "CompositeAlarms",
    "Contributors",
    "DashboardEntries",
    "DashboardValidationMessages",
    "Datapoints",
    "Dimensions",
    "Entries",
    "ExcludeFilters",
    "ExcludedTimeRanges",
    "Failures",
    "IncludeFilters",
    "IncludeMetrics",
    "InsightRules",
    "InsufficientDataActions",
    "KeyLabels",
    "Keys",
    "LogAlarms",
    "LogGroupIdentifiers",
    "ManagedRules",
    "Messages",
    "MetricAlarms",
    "MetricDataQueries",
    "MetricDataResults",
    "MetricDatapoints",
    "MetricNames",
    "MetricSelections",
    "Metrics",
    "OKActions",
    "OwningAccounts",
    "StatisticsConfigurations",
    "Tags",
    "Timestamps",
    "Values",
    "dashboardValidationMessages",
];

/// Output members whose leaf text is a boolean.
const BOOL_TAGS: &[&str] = &[
    "ActionsEnabled",
    "ApplyOnTransformedLogs",
    "IncludeLinkedAccountsMetrics",
    "ManagedRule",
    "OnlyStartEvaluatingAfterWarmUpPeriodEnds",
    "PeriodicSpikes",
    "ReturnData",
];

/// Output members whose leaf text is an ISO-8601 timestamp, rendered as epoch
/// seconds (a tag-1 value in CBOR).
const TIMESTAMP_TAGS: &[&str] = &[
    "AlarmConfigurationUpdatedTimestamp",
    "CreatedAt",
    "CreationDate",
    "EndTime",
    "ExpireDate",
    "LastModified",
    "LastUpdateDate",
    "LastUpdatedTimestamp",
    "StartDate",
    "StartTime",
    "StateTransitionedTimestamp",
    "StateUpdatedTimestamp",
    "Timestamp",
    "Timestamps",
    "UpdatedAt",
];

/// Output members that are blobs: base64 text in awsQuery XML and awsJson, a
/// byte string in CBOR.
const BLOB_TAGS: &[&str] = &["MetricWidgetImage"];

/// A response value typed against the output schema, rendered as JSON or CBOR.
#[derive(Debug, Clone, PartialEq)]
enum Typed {
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    /// Epoch seconds, millisecond precision.
    Timestamp(f64),
    Blob(Vec<u8>),
    List(Vec<Typed>),
    /// A structure or a map: ordered members.
    Object(Vec<(String, Typed)>),
}

impl Typed {
    fn to_json(&self) -> Value {
        match self {
            Typed::Str(s) => Value::String(s.clone()),
            Typed::Int(i) => Value::Number((*i).into()),
            Typed::Double(f) | Typed::Timestamp(f) => match serde_json::Number::from_f64(*f) {
                Some(n) => Value::Number(n),
                None if f.is_nan() => Value::String("NaN".to_string()),
                None if *f > 0.0 => Value::String("Infinity".to_string()),
                None => Value::String("-Infinity".to_string()),
            },
            Typed::Bool(b) => Value::Bool(*b),
            Typed::Blob(b) => Value::String(base64::engine::general_purpose::STANDARD.encode(b)),
            Typed::List(items) => Value::Array(items.iter().map(Typed::to_json).collect()),
            Typed::Object(members) => Value::Object(
                members
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_json()))
                    .collect::<Map<String, Value>>(),
            ),
        }
    }

    fn to_cbor(&self) -> CborValue {
        match self {
            Typed::Str(s) => CborValue::Text(s.clone()),
            Typed::Int(i) => CborValue::Integer((*i).into()),
            Typed::Double(f) => CborValue::Float(*f),
            Typed::Timestamp(secs) => cbor::timestamp(*secs),
            Typed::Bool(b) => CborValue::Bool(*b),
            Typed::Blob(b) => CborValue::Bytes(b.clone()),
            Typed::List(items) => CborValue::Array(items.iter().map(Typed::to_cbor).collect()),
            Typed::Object(members) => CborValue::Map(
                members
                    .iter()
                    .map(|(k, v)| (CborValue::Text(k.clone()), v.to_cbor()))
                    .collect(),
            ),
        }
    }
}

/// Rebuild an XML awsQuery response as an awsJson1_0 response, preserving the
/// original HTTP status.
pub(crate) fn xml_response_to_json(resp: AwsResponse) -> AwsResponse {
    rebuild(resp, JSON_CONTENT_TYPE, |typed| {
        serde_json::to_vec(&typed.to_json()).unwrap_or_else(|_| b"{}".to_vec())
    })
}

/// Rebuild an XML awsQuery response as an rpcv2Cbor response, preserving the
/// original HTTP status. The dispatcher adds the `smithy-protocol` header.
pub(crate) fn xml_response_to_cbor(resp: AwsResponse) -> AwsResponse {
    rebuild(resp, cbor::CBOR_CONTENT_TYPE, |typed| {
        cbor::encode(&typed.to_cbor())
    })
}

fn rebuild(
    resp: AwsResponse,
    content_type: &str,
    render: impl Fn(&Typed) -> Vec<u8>,
) -> AwsResponse {
    let status = resp.status;
    let ResponseBody::Bytes(bytes) = resp.body else {
        // CloudWatch handlers never stream a file body; fall back untouched.
        return resp;
    };
    let body = render(&xml_to_typed(&bytes));
    AwsResponse {
        status,
        content_type: content_type.to_string(),
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

/// Parse the awsQuery XML envelope and type its `<{Action}Result>` body.
/// Returns an empty object when there is no result body (e.g. metadata-only
/// responses like `PutMetricData`).
fn xml_to_typed(xml: &[u8]) -> Typed {
    let empty = Typed::Object(Vec::new());
    let Some(root) = parse_xml(xml) else {
        return empty;
    };
    // The result body lives in the `<{Action}Result>` child; `ResponseMetadata`
    // is dropped.
    let Some(result) = root.children.iter().find(|c| c.name.ends_with("Result")) else {
        return empty;
    };
    match convert(result, "") {
        Some(v @ Typed::Object(_)) => v,
        _ => empty,
    }
}

#[cfg(test)]
fn xml_to_json(xml: &[u8]) -> Value {
    xml_to_typed(xml).to_json()
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

/// Convert an element to a typed value. `parent_tag` carries the enclosing
/// list/map tag so scalar `<member>` leaves can be typed against the field
/// they belong to.
fn convert(el: &El, parent_tag: &str) -> Option<Typed> {
    if el.children.is_empty() {
        let text = el.text.trim();
        if text.is_empty() {
            // An empty list container (`<Datapoints></Datapoints>`) carries no
            // `<member>` children, so it lands here rather than the list branch
            // below. Emit `[]` for known list tags so callers see an empty
            // array instead of a missing field; a genuinely empty scalar is
            // still omitted (absent members are dropped on both protocols).
            if LIST_TAGS.contains(&el.name.as_str()) {
                return Some(Typed::List(Vec::new()));
            }
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
        let items = el
            .children
            .iter()
            .filter_map(|m| convert(m, &el.name))
            .collect();
        return Some(Typed::List(items));
    }

    // awsQuery map: every child is `<entry>` with `<key>`/`<value>`.
    if el.children.iter().all(|c| c.name == "entry") {
        let mut members = Vec::new();
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
                members.push((k, v));
            }
        }
        return Some(Typed::Object(members));
    }

    // Plain structure.
    let members = el
        .children
        .iter()
        .filter_map(|child| convert(child, &el.name).map(|v| (child.name.clone(), v)))
        .collect();
    Some(Typed::Object(members))
}

/// Type a leaf value per the CloudWatch output schema. Text that does not
/// parse as the schema type is kept as a string rather than dropped.
fn type_leaf(tag: &str, text: &str) -> Typed {
    if TIMESTAMP_TAGS.contains(&tag) {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(text) {
            return Typed::Timestamp(dt.timestamp_millis() as f64 / 1000.0);
        }
    } else if BOOL_TAGS.contains(&tag) {
        match text {
            "true" => return Typed::Bool(true),
            "false" => return Typed::Bool(false),
            _ => {}
        }
    } else if DOUBLE_TAGS.contains(&tag) {
        if let Ok(f) = text.parse::<f64>() {
            return Typed::Double(f);
        }
    } else if INTEGER_TAGS.contains(&tag) {
        if let Ok(i) = text.parse::<i64>() {
            return Typed::Int(i);
        }
    } else if BLOB_TAGS.contains(&tag) {
        if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(text) {
            return Typed::Blob(bytes);
        }
    }
    Typed::Str(text.to_string())
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
        assert_eq!(v["Datapoints"][0]["SampleCount"], 3.0);
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

    #[test]
    fn resource_metrics_configuration_timestamps_typed() {
        let inner = "<ResourceMetricsConfiguration><ResourceArn>arn:aws:ec2:us-east-1:123456789012:instance/i-1</ResourceArn>\
            <CreatedAt>2020-01-01T00:00:00.000Z</CreatedAt><UpdatedAt>2020-01-01T00:00:01.000Z</UpdatedAt>\
            <MetricSelections><member><IncludeMetrics><member>CPUUtilization</member></IncludeMetrics></member></MetricSelections>\
            </ResourceMetricsConfiguration>";
        let v = json("GetResourceMetricsConfiguration", inner);
        let cfg = &v["ResourceMetricsConfiguration"];
        assert_eq!(cfg["CreatedAt"], 1577836800.0);
        assert_eq!(cfg["UpdatedAt"], 1577836801.0);
        assert_eq!(
            cfg["MetricSelections"][0]["IncludeMetrics"][0],
            "CPUUtilization"
        );
    }

    #[test]
    fn empty_list_container_becomes_empty_array() {
        // An explicitly-empty list in the XML must serialize as [], not be
        // dropped. A genuinely empty scalar (NextToken) stays omitted.
        let inner = "<Label>cpu</Label><Datapoints></Datapoints><NextToken></NextToken>";
        let v = json("GetMetricStatistics", inner);
        assert_eq!(v["Label"], "cpu");
        assert_eq!(v["Datapoints"], serde_json::json!([]));
        assert!(v["Datapoints"].is_array());
        assert!(
            v.get("NextToken").is_none(),
            "empty scalar should be omitted, got {v}"
        );
    }

    #[test]
    fn empty_tags_and_messages_lists_become_empty_arrays() {
        let tags = json("ListTagsForResource", "<Tags></Tags>");
        assert_eq!(tags["Tags"], serde_json::json!([]));

        // Nested empty list inside a member.
        let inner = "<MetricDataResults><member><Id>m1</Id>\
            <Messages></Messages><Values></Values></member></MetricDataResults>";
        let v = json("GetMetricData", inner);
        assert_eq!(v["MetricDataResults"][0]["Messages"], serde_json::json!([]));
        assert_eq!(v["MetricDataResults"][0]["Values"], serde_json::json!([]));
    }

    fn cbor_of(action: &str, inner: &str) -> CborValue {
        let xml = fakecloud_core::query::query_response_xml(
            action,
            "http://monitoring.amazonaws.com/doc/2010-08-01/",
            inner,
            "req-1",
        );
        let resp = xml_response_to_cbor(AwsResponse::xml(http::StatusCode::OK, xml));
        assert_eq!(resp.content_type, "application/cbor");
        let ResponseBody::Bytes(bytes) = resp.body else {
            panic!("expected bytes");
        };
        ciborium::from_reader(bytes.as_ref()).unwrap()
    }

    fn field<'a>(v: &'a CborValue, key: &str) -> &'a CborValue {
        let CborValue::Map(entries) = v else {
            panic!("expected map, got {v:?}");
        };
        entries
            .iter()
            .find(|(k, _)| k == &CborValue::Text(key.to_string()))
            .map(|(_, v)| v)
            .unwrap_or_else(|| panic!("missing {key} in {v:?}"))
    }

    fn index(v: &CborValue, i: usize) -> &CborValue {
        let CborValue::Array(items) = v else {
            panic!("expected array, got {v:?}");
        };
        &items[i]
    }

    #[test]
    fn cbor_types_follow_the_output_schema() {
        let inner = "<Label>cpu</Label><Datapoints><member>\
            <Timestamp>2020-01-01T00:00:00.500Z</Timestamp>\
            <SampleCount>3</SampleCount><Average>2</Average>\
            <ExtendedStatistics><entry><key>p99</key><value>7</value></entry></ExtendedStatistics>\
            </member></Datapoints>";
        let v = cbor_of("GetMetricStatistics", inner);
        assert_eq!(field(&v, "Label"), &CborValue::Text("cpu".to_string()));
        let dp = index(field(&v, "Datapoints"), 0);
        // Timestamps are tag 1 over epoch seconds.
        assert_eq!(
            field(dp, "Timestamp"),
            &CborValue::Tag(1, Box::new(CborValue::Float(1_577_836_800.5)))
        );
        // Doubles are floats even when the XML text is integral.
        assert_eq!(field(dp, "SampleCount"), &CborValue::Float(3.0));
        assert_eq!(field(dp, "Average"), &CborValue::Float(2.0));
        assert_eq!(
            field(field(dp, "ExtendedStatistics"), "p99"),
            &CborValue::Float(7.0)
        );

        let alarms = cbor_of(
            "DescribeAlarms",
            "<MetricAlarms><member><Period>60</Period><ActionsEnabled>true</ActionsEnabled>\
             <AlarmActions></AlarmActions></member></MetricAlarms>",
        );
        let alarm = index(field(&alarms, "MetricAlarms"), 0);
        assert_eq!(field(alarm, "Period"), &CborValue::Integer(60.into()));
        assert_eq!(field(alarm, "ActionsEnabled"), &CborValue::Bool(true));
        assert_eq!(field(alarm, "AlarmActions"), &CborValue::Array(vec![]));
    }

    #[test]
    fn cbor_blob_is_a_byte_string() {
        let v = cbor_of(
            "GetMetricWidgetImage",
            "<MetricWidgetImage>aGVsbG8=</MetricWidgetImage>",
        );
        assert_eq!(
            field(&v, "MetricWidgetImage"),
            &CborValue::Bytes(b"hello".to_vec())
        );
        // awsJson keeps the base64 text.
        let j = json(
            "GetMetricWidgetImage",
            "<MetricWidgetImage>aGVsbG8=</MetricWidgetImage>",
        );
        assert_eq!(j["MetricWidgetImage"], "aGVsbG8=");
    }

    #[test]
    fn insight_rule_report_numbers_are_typed() {
        let inner = "<KeyLabels></KeyLabels><AggregationStatistic>Sum</AggregationStatistic>\
            <AggregateValue>0.0</AggregateValue><ApproximateUniqueCount>0</ApproximateUniqueCount>\
            <Contributors/><MetricDatapoints/>";
        let v = json("GetInsightRuleReport", inner);
        assert_eq!(v["AggregateValue"], 0.0);
        assert!(v["AggregateValue"].is_f64());
        assert_eq!(v["ApproximateUniqueCount"], 0);
        assert!(v["ApproximateUniqueCount"].is_i64());
        assert_eq!(v["KeyLabels"], serde_json::json!([]));
        assert_eq!(v["Contributors"], serde_json::json!([]));
        assert_eq!(v["MetricDatapoints"], serde_json::json!([]));
    }

    /// Classify every member reachable from an operation output (or error) in
    /// the CloudWatch Smithy model, keyed by member name.
    fn model_member_kinds() -> std::collections::BTreeMap<String, std::collections::BTreeSet<String>>
    {
        use std::collections::{BTreeMap, BTreeSet};
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../aws-models/cloudwatch.json");
        let model: Value = serde_json::from_slice(
            &std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display())),
        )
        .unwrap();
        let shapes = model["shapes"].as_object().unwrap();
        let kind_of = |target: &str| -> String {
            match shapes.get(target) {
                Some(shape) => shape["type"].as_str().unwrap().to_string(),
                None => target
                    .trim_start_matches("smithy.api#")
                    .trim_start_matches("Primitive")
                    .to_ascii_lowercase(),
            }
        };
        let mut kinds: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut stack: Vec<String> = Vec::new();
        let service = shapes
            .values()
            .find(|s| s["type"] == "service")
            .expect("service shape");
        for op in service["operations"].as_array().unwrap() {
            let op = &shapes[op["target"].as_str().unwrap()];
            if let Some(out) = op["output"]["target"].as_str() {
                stack.push(out.to_string());
            }
            for err in op["errors"].as_array().into_iter().flatten() {
                stack.push(err["target"].as_str().unwrap().to_string());
            }
        }
        while let Some(id) = stack.pop() {
            if !seen.insert(id.clone()) {
                continue;
            }
            let Some(shape) = shapes.get(&id) else {
                continue;
            };
            for (name, member) in shape["members"].as_object().into_iter().flatten() {
                let target = member["target"].as_str().unwrap();
                let kind = match kind_of(target).as_str() {
                    "list" => {
                        let elem = shapes[target]["member"]["target"].as_str().unwrap();
                        stack.push(elem.to_string());
                        format!("list:{}", kind_of(elem))
                    }
                    "map" => {
                        let value = shapes[target]["value"]["target"].as_str().unwrap();
                        stack.push(value.to_string());
                        format!("map:{}", kind_of(value))
                    }
                    "structure" | "union" => {
                        stack.push(target.to_string());
                        "structure".to_string()
                    }
                    other => other.to_string(),
                };
                kinds.entry(name.clone()).or_default().insert(kind);
            }
        }
        kinds
    }

    #[test]
    fn member_type_tables_match_the_smithy_model() {
        use std::collections::BTreeSet;
        let kinds = model_member_kinds();
        // The scalar kind a member's leaves carry: its own type, or its list
        // elements' / map values' type.
        let leaf_kinds = |name: &str| -> BTreeSet<String> {
            kinds[name]
                .iter()
                .map(|k| k.rsplit(':').next().unwrap().to_string())
                .collect()
        };
        let expect = |table: &[&str], wanted: &[&str], label: &str| {
            let from_model: BTreeSet<&str> = kinds
                .keys()
                .map(String::as_str)
                .filter(|name| {
                    leaf_kinds(name)
                        .iter()
                        .any(|k| wanted.contains(&k.as_str()))
                })
                .collect();
            let ours: BTreeSet<&str> = table.iter().copied().collect();
            assert_eq!(ours, from_model, "{label} table out of sync with the model");
        };
        expect(DOUBLE_TAGS, &["double", "float"], "DOUBLE_TAGS");
        expect(
            INTEGER_TAGS,
            &["integer", "long", "short", "byte"],
            "INTEGER_TAGS",
        );
        expect(BOOL_TAGS, &["boolean"], "BOOL_TAGS");
        expect(TIMESTAMP_TAGS, &["timestamp"], "TIMESTAMP_TAGS");
        expect(BLOB_TAGS, &["blob"], "BLOB_TAGS");
        let lists: BTreeSet<&str> = kinds
            .iter()
            .filter(|(_, k)| k.iter().any(|k| k.starts_with("list:")))
            .map(|(n, _)| n.as_str())
            .collect();
        let ours: BTreeSet<&str> = LIST_TAGS.iter().copied().collect();
        assert_eq!(ours, lists, "LIST_TAGS out of sync with the model");
        // Typing is by member name, so every typed name must mean one scalar
        // kind wherever it appears.
        for name in DOUBLE_TAGS
            .iter()
            .chain(INTEGER_TAGS)
            .chain(BOOL_TAGS)
            .chain(TIMESTAMP_TAGS)
            .chain(BLOB_TAGS)
        {
            assert_eq!(
                leaf_kinds(name).len(),
                1,
                "{name} has mixed types in the model"
            );
        }
    }
}
