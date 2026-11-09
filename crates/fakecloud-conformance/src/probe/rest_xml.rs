//! restXml request-body serialization for the probe.
//!
//! The generators produce every variant input as JSON. restXml services (S3,
//! Route 53, CloudFront) read XML bodies, so the probe must serialize the body
//! members the way an AWS SDK does, or the handler rejects the request before
//! running and the variant scores a modelled 400 without exercising anything.
//!
//! Rules (Smithy restXml / XML binding traits):
//! - An `@httpPayload` structure or union is the whole body; its root element
//!   is named by the member's `@xmlName`, else the target's `@xmlName`, else
//!   the target shape name. Without a payload member, the unbound members are
//!   wrapped in an element named after the input shape (`@xmlName` or name).
//! - The root carries the member's, else the target's, else the service's
//!   `@xmlNamespace`.
//! - Structure members are child elements named by the member's `@xmlName`
//!   (else the member name); `@xmlAttribute` members are attributes of the
//!   parent element instead.
//! - Lists wrap each item in `<member>` (or the list member's `@xmlName`);
//!   `@xmlFlattened` lists repeat the member's own element per item.
//! - Maps wrap each pair in `<entry><key/><value/></entry>` (key/value names
//!   from their `@xmlName`); `@xmlFlattened` maps repeat the member element
//!   per pair with no `<entry>`.
//! - Timestamps default to `date-time`; blobs are base64 text; booleans and
//!   numbers are their literal text.

use crate::smithy::{Member, ServiceModel, ShapeTraits, ShapeType, XmlNamespace};
use serde_json::Value;

/// Whether an input member is bound to the HTTP request outside the body
/// (label, query, header, prefix headers, query-params map). Traits can sit on
/// the member or on its target shape.
fn is_non_body_bound(model: &ServiceModel, member: &Member) -> bool {
    let bound = |t: &ShapeTraits| {
        t.http_label
            || t.http_query.is_some()
            || t.http_header.is_some()
            || t.http_prefix_headers.is_some()
            || t.http_query_params
    };
    bound(&member.traits)
        || model
            .shapes
            .get(&member.target)
            .is_some_and(|s| bound(&s.traits))
}

fn is_payload(model: &ServiceModel, member: &Member) -> bool {
    member.traits.http_payload
        || model
            .shapes
            .get(&member.target)
            .is_some_and(|s| s.traits.http_payload)
}

/// The restXml request body for `op` with `input`, and its `Content-Type`.
///
/// - `@httpPayload` structure/union: an XML document rooted at the payload.
/// - `@httpPayload` blob/string: the raw value.
/// - No payload member: the unbound members wrapped in the input shape's
///   element, or no body when the input has no unbound members at all.
///
/// An omitted payload member sends no body, as the SDKs do.
pub(super) fn rest_xml_body(
    op: &crate::smithy::Operation,
    model: &ServiceModel,
    input: &Value,
) -> (Option<String>, Option<&'static str>) {
    let Some(input_shape) = op.input_shape.as_deref() else {
        return (None, None);
    };
    let members: &[Member] = match model.shapes.get(input_shape).map(|s| &s.shape_type) {
        Some(ShapeType::Structure { members }) => members,
        _ => return (None, None),
    };
    let obj = input.as_object();

    if let Some(payload) = members.iter().find(|m| is_payload(model, m)) {
        let Some(value) = obj.and_then(|o| o.get(&payload.name)) else {
            return (None, None);
        };
        if value.is_null() {
            return (None, None);
        }
        return match model.shapes.get(&payload.target).map(|s| &s.shape_type) {
            Some(ShapeType::Structure { .. }) | Some(ShapeType::Union { .. }) => {
                let (name, ns) = payload_root(model, payload);
                (
                    Some(serialize_root(
                        model,
                        &name,
                        ns.as_ref(),
                        &payload.target,
                        value,
                    )),
                    Some("application/xml"),
                )
            }
            Some(ShapeType::Blob) => (json_scalar_text(value), Some("application/octet-stream")),
            _ => (json_scalar_text(value), Some("text/plain")),
        };
    }

    let body_members: Vec<&Member> = members
        .iter()
        .filter(|m| !is_non_body_bound(model, m))
        .collect();
    if body_members.is_empty() {
        return (None, None);
    }
    let (name, ns) = input_root(model, input_shape);
    let mut body_value = serde_json::Map::new();
    if let Some(obj) = obj {
        for (k, v) in obj {
            let bound_elsewhere = members
                .iter()
                .any(|m| &m.name == k && is_non_body_bound(model, m));
            if !bound_elsewhere {
                body_value.insert(k.clone(), v.clone());
            }
        }
    }
    (
        Some(serialize_root(
            model,
            &name,
            ns.as_ref(),
            input_shape,
            &Value::Object(body_value),
        )),
        Some("application/xml"),
    )
}

/// Serialize `value` as an XML document whose root element is `root_name`
/// and whose content follows `shape_id`.
pub(super) fn serialize_root(
    model: &ServiceModel,
    root_name: &str,
    namespace: Option<&XmlNamespace>,
    shape_id: &str,
    value: &Value,
) -> String {
    let mut out = String::new();
    write_element(model, &mut out, root_name, namespace, shape_id, value, None);
    out
}

/// Root element name and namespace for an `@httpPayload` member.
pub(super) fn payload_root(
    model: &ServiceModel,
    member: &Member,
) -> (String, Option<XmlNamespace>) {
    let target = model.shapes.get(&member.target);
    let name = member
        .traits
        .xml_name
        .clone()
        .or_else(|| target.and_then(|t| t.traits.xml_name.clone()))
        .unwrap_or_else(|| crate::smithy::short_name(&member.target).to_string());
    let ns = member
        .traits
        .xml_namespace
        .clone()
        .or_else(|| target.and_then(|t| t.traits.xml_namespace.clone()))
        .or_else(|| service_namespace(model));
    (name, ns)
}

/// Root element name and namespace for an input shape with unbound members.
pub(super) fn input_root(
    model: &ServiceModel,
    input_shape: &str,
) -> (String, Option<XmlNamespace>) {
    let shape = model.shapes.get(input_shape);
    let name = shape
        .and_then(|s| s.traits.xml_name.clone())
        .unwrap_or_else(|| crate::smithy::short_name(input_shape).to_string());
    let ns = shape
        .and_then(|s| s.traits.xml_namespace.clone())
        .or_else(|| service_namespace(model));
    (name, ns)
}

fn service_namespace(model: &ServiceModel) -> Option<XmlNamespace> {
    model
        .service_shape()
        .and_then(|s| s.traits.xml_namespace.clone())
}

/// Write `<name ...>content</name>` for `value` of shape `shape_id`.
/// `member_traits` are the traits of the structure member targeting this
/// element, used for its timestamp format.
fn write_element(
    model: &ServiceModel,
    out: &mut String,
    name: &str,
    namespace: Option<&XmlNamespace>,
    shape_id: &str,
    value: &Value,
    member_traits: Option<&ShapeTraits>,
) {
    if value.is_null() {
        return;
    }
    let shape = model.shapes.get(shape_id);
    out.push('<');
    out.push_str(name);
    if let Some(ns) = namespace {
        push_namespace(out, ns);
    }
    match shape.map(|s| &s.shape_type) {
        Some(ShapeType::Structure { members }) | Some(ShapeType::Union { members }) => {
            let members = declared_order(members, shape.map(|s| &s.traits));
            let obj = value.as_object();
            // Attributes go on the open tag.
            if let Some(obj) = obj {
                for m in members.iter().filter(|m| m.traits.xml_attribute) {
                    if let Some(v) = obj.get(&m.name) {
                        if let Some(text) = scalar_text(model, &m.target, v, Some(&m.traits)) {
                            out.push(' ');
                            out.push_str(member_xml_name(m));
                            out.push_str("=\"");
                            out.push_str(&escape(&text));
                            out.push('"');
                        }
                    }
                }
            }
            out.push('>');
            match obj {
                Some(obj) => {
                    for m in members.iter().filter(|m| !m.traits.xml_attribute) {
                        if let Some(v) = obj.get(&m.name) {
                            write_member(model, out, m, v);
                        }
                    }
                    // Keys the model does not declare still reach the wire, so
                    // a variant never silently loses part of its input.
                    for (k, v) in obj {
                        if !members.iter().any(|m| &m.name == k) {
                            write_untyped(out, k, v);
                        }
                    }
                }
                None => {
                    // A scalar where a structure is modelled: send it as text
                    // so the handler sees (and rejects) the malformed value.
                    if let Some(text) = json_scalar_text(value) {
                        out.push_str(&escape(&text));
                    }
                }
            }
        }
        Some(ShapeType::List { member_target }) => {
            out.push('>');
            let item_name = shape
                .and_then(|s| s.traits.xml_list_member_name.as_deref())
                .unwrap_or("member");
            write_list_items(model, out, item_name, member_target, value);
        }
        Some(ShapeType::Map {
            key_target,
            value_target,
        }) => {
            out.push('>');
            let traits = shape.map(|s| &s.traits);
            if let Some(obj) = value.as_object() {
                for (k, v) in obj {
                    out.push_str("<entry>");
                    write_map_pair(model, out, traits, key_target, value_target, k, v);
                    out.push_str("</entry>");
                }
            }
        }
        _ => {
            out.push('>');
            if let Some(text) = scalar_text(model, shape_id, value, member_traits) {
                out.push_str(&escape(&text));
            }
        }
    }
    out.push_str("</");
    out.push_str(name);
    out.push('>');
}

/// Write one structure member (child element, or repeated elements for a
/// flattened collection).
fn write_member(model: &ServiceModel, out: &mut String, member: &Member, value: &Value) {
    let name = member_xml_name(member);
    let ns = member.traits.xml_namespace.as_ref();
    if member.traits.xml_flattened {
        let shape = model.shapes.get(&member.target);
        match shape.map(|s| &s.shape_type) {
            Some(ShapeType::List { member_target }) => {
                write_list_items(model, out, name, member_target, value);
                return;
            }
            Some(ShapeType::Map {
                key_target,
                value_target,
            }) => {
                if let Some(obj) = value.as_object() {
                    for (k, v) in obj {
                        out.push('<');
                        out.push_str(name);
                        if let Some(ns) = ns {
                            push_namespace(out, ns);
                        }
                        out.push('>');
                        write_map_pair(
                            model,
                            out,
                            shape.map(|s| &s.traits),
                            key_target,
                            value_target,
                            k,
                            v,
                        );
                        out.push_str("</");
                        out.push_str(name);
                        out.push('>');
                    }
                }
                return;
            }
            _ => {}
        }
    }
    write_element(
        model,
        out,
        name,
        ns,
        &member.target,
        value,
        Some(&member.traits),
    );
}

/// `members` in the model's declaration order. AWS SDKs serialize elements
/// in that order, and some services validate bodies against an XSD sequence.
fn declared_order<'a>(members: &'a [Member], traits: Option<&ShapeTraits>) -> Vec<&'a Member> {
    let mut ordered: Vec<&Member> = members.iter().collect();
    if let Some(order) = traits.map(|t| &t.member_order).filter(|o| !o.is_empty()) {
        ordered.sort_by_key(|m| {
            order
                .iter()
                .position(|n| n == &m.name)
                .unwrap_or(usize::MAX)
        });
    }
    ordered
}

fn write_list_items(
    model: &ServiceModel,
    out: &mut String,
    item_name: &str,
    member_target: &str,
    value: &Value,
) {
    match value {
        Value::Array(items) => {
            for item in items {
                write_element(model, out, item_name, None, member_target, item, None);
            }
        }
        // A non-array where a list is modelled: send it as a single item.
        other => write_element(model, out, item_name, None, member_target, other, None),
    }
}

fn write_map_pair(
    model: &ServiceModel,
    out: &mut String,
    map_traits: Option<&ShapeTraits>,
    key_target: &str,
    value_target: &str,
    key: &str,
    value: &Value,
) {
    let key_name = map_traits
        .and_then(|t| t.xml_map_key_name.as_deref())
        .unwrap_or("key");
    let value_name = map_traits
        .and_then(|t| t.xml_map_value_name.as_deref())
        .unwrap_or("value");
    write_element(
        model,
        out,
        key_name,
        None,
        key_target,
        &Value::String(key.to_string()),
        None,
    );
    write_element(model, out, value_name, None, value_target, value, None);
}

/// Serialize a JSON value with no model shape behind it (an input key the
/// model does not declare).
fn write_untyped(out: &mut String, name: &str, value: &Value) {
    match value {
        Value::Null => {}
        Value::Array(items) => {
            for item in items {
                write_untyped(out, name, item);
            }
        }
        Value::Object(obj) => {
            out.push('<');
            out.push_str(name);
            out.push('>');
            for (k, v) in obj {
                write_untyped(out, k, v);
            }
            out.push_str("</");
            out.push_str(name);
            out.push('>');
        }
        scalar => {
            out.push('<');
            out.push_str(name);
            out.push('>');
            if let Some(text) = json_scalar_text(scalar) {
                out.push_str(&escape(&text));
            }
            out.push_str("</");
            out.push_str(name);
            out.push('>');
        }
    }
}

fn member_xml_name(member: &Member) -> &str {
    member.traits.xml_name.as_deref().unwrap_or(&member.name)
}

fn push_namespace(out: &mut String, ns: &XmlNamespace) {
    match &ns.prefix {
        Some(prefix) => {
            out.push_str(" xmlns:");
            out.push_str(prefix);
        }
        None => out.push_str(" xmlns"),
    }
    out.push_str("=\"");
    out.push_str(&escape(&ns.uri));
    out.push('"');
}

/// The text form of a scalar value. Timestamps honour the member's (else the
/// shape's) `@timestampFormat`, defaulting to `date-time`.
fn scalar_text(
    model: &ServiceModel,
    shape_id: &str,
    value: &Value,
    member_traits: Option<&ShapeTraits>,
) -> Option<String> {
    let is_timestamp = shape_id == "smithy.api#Timestamp"
        || matches!(
            model.shapes.get(shape_id).map(|s| &s.shape_type),
            Some(ShapeType::Timestamp)
        );
    if is_timestamp {
        let format = member_traits
            .and_then(|t| t.timestamp_format.clone())
            .or_else(|| {
                model
                    .shapes
                    .get(shape_id)
                    .and_then(|s| s.traits.timestamp_format.clone())
            });
        if let Some(s) = value.as_str() {
            return Some(format_timestamp(s, format.as_deref()));
        }
    }
    json_scalar_text(value)
}

fn json_scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Re-render an RFC 3339 `YYYY-MM-DDTHH:MM:SS[.fff]Z` timestamp (the form the
/// generators emit) in `format`. Anything that does not parse is sent
/// unchanged, so a deliberately malformed value still reaches the handler.
fn format_timestamp(s: &str, format: Option<&str>) -> String {
    let Some(epoch) = parse_rfc3339_utc(s) else {
        return s.to_string();
    };
    match format {
        Some("epoch-seconds") => epoch.to_string(),
        Some("http-date") => http_date(epoch),
        _ => s.to_string(),
    }
}

fn parse_rfc3339_utc(s: &str) -> Option<i64> {
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let mut d = date.splitn(3, '-');
    let year: i64 = d.next()?.parse().ok()?;
    let month: i64 = d.next()?.parse().ok()?;
    let day: i64 = d.next()?.parse().ok()?;
    let time = time.split('.').next()?;
    let mut t = time.splitn(3, ':');
    let hour: i64 = t.next()?.parse().ok()?;
    let minute: i64 = t.next()?.parse().ok()?;
    let second: i64 = t.next()?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second)
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn http_date(epoch: i64) -> String {
    const DAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = epoch.div_euclid(86_400);
    let secs = epoch.rem_euclid(86_400);
    // Inverse of `days_from_civil`.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        DAYS[days.rem_euclid(7) as usize],
        day,
        MONTHS[(month - 1) as usize],
        year,
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            // Control characters as character references, as the AWS SDK
            // XML writers do, so line endings survive XML normalization.
            c if (c as u32) < 0x20 && c != '\t' => {
                out.push_str(&format!("&#x{:X};", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::smithy::{parse_model_str, Operation};
    use serde_json::json;

    /// A restXml service with one payload operation and one wrapped-body
    /// operation, covering every XML binding trait the serializer handles.
    fn model() -> ServiceModel {
        parse_model_str(
            r##"{
                "smithy": "2.0",
                "shapes": {
                    "t#Svc": {
                        "type": "service",
                        "operations": [{"target": "t#PutThing"}, {"target": "t#CreateWidget"}],
                        "traits": {
                            "aws.protocols#restXml": {},
                            "smithy.api#xmlNamespace": {"uri": "https://svc.example/doc/"}
                        }
                    },
                    "t#PutThing": {
                        "type": "operation",
                        "input": {"target": "t#PutThingRequest"},
                        "traits": {"smithy.api#http": {"method": "PUT", "uri": "/thing/{Id}"}}
                    },
                    "t#PutThingRequest": {
                        "type": "structure",
                        "members": {
                            "Id": {"target": "t#Str", "traits": {"smithy.api#httpLabel": {}, "smithy.api#required": {}}},
                            "Token": {"target": "t#Str", "traits": {"smithy.api#httpHeader": "x-token"}},
                            "Config": {
                                "target": "t#ThingConfig",
                                "traits": {"smithy.api#httpPayload": {}, "smithy.api#xmlName": "ThingConfiguration"}
                            }
                        }
                    },
                    "t#ThingConfig": {
                        "type": "structure",
                        "members": {
                            "Kind": {"target": "t#Str", "traits": {"smithy.api#xmlAttribute": {}, "smithy.api#xmlName": "kind"}},
                            "Name": {"target": "t#Str"},
                            "Enabled": {"target": "t#Bool"},
                            "Count": {"target": "t#Int"},
                            "Rule": {"target": "t#RuleList", "traits": {"smithy.api#xmlFlattened": {}}},
                            "Aliases": {"target": "t#AliasList"},
                            "Labels": {"target": "t#LabelMap"},
                            "Header": {"target": "t#HeaderMap", "traits": {"smithy.api#xmlFlattened": {}}},
                            "Data": {"target": "t#Blob"},
                            "At": {"target": "t#Time"},
                            "AtEpoch": {"target": "t#Time", "traits": {"smithy.api#timestampFormat": "epoch-seconds"}},
                            "AtHttp": {"target": "t#Time", "traits": {"smithy.api#timestampFormat": "http-date"}},
                            "Grantee": {
                                "target": "t#Grantee",
                                "traits": {"smithy.api#xmlNamespace": {"uri": "http://www.w3.org/2001/XMLSchema-instance", "prefix": "xsi"}}
                            }
                        }
                    },
                    "t#Grantee": {
                        "type": "structure",
                        "members": {
                            "Type": {"target": "t#Str", "traits": {"smithy.api#xmlAttribute": {}, "smithy.api#xmlName": "xsi:type"}},
                            "ID": {"target": "t#Str"}
                        }
                    },
                    "t#RuleList": {"type": "list", "member": {"target": "t#Str"}},
                    "t#AliasList": {"type": "list", "member": {"target": "t#Str", "traits": {"smithy.api#xmlName": "CNAME"}}},
                    "t#LabelMap": {
                        "type": "map",
                        "key": {"target": "t#Str", "traits": {"smithy.api#xmlName": "K"}},
                        "value": {"target": "t#Str", "traits": {"smithy.api#xmlName": "V"}}
                    },
                    "t#HeaderMap": {"type": "map", "key": {"target": "t#Str"}, "value": {"target": "t#Str"}},
                    "t#CreateWidget": {
                        "type": "operation",
                        "input": {"target": "t#CreateWidgetRequest"},
                        "traits": {"smithy.api#http": {"method": "POST", "uri": "/widget/{Id}"}}
                    },
                    "t#CreateWidgetRequest": {
                        "type": "structure",
                        "members": {
                            "Id": {"target": "t#Str", "traits": {"smithy.api#httpLabel": {}, "smithy.api#required": {}}},
                            "Marker": {"target": "t#Str", "traits": {"smithy.api#httpQuery": "marker"}},
                            "Name": {"target": "t#Str"},
                            "Comment": {"target": "t#Str"}
                        }
                    },
                    "t#Str": {"type": "string"},
                    "t#Bool": {"type": "boolean"},
                    "t#Int": {"type": "integer"},
                    "t#Blob": {"type": "blob"},
                    "t#Time": {"type": "timestamp"}
                }
            }"##,
        )
        .expect("parse test model")
    }

    fn op<'a>(m: &'a ServiceModel, name: &str) -> &'a Operation {
        m.operations.iter().find(|o| o.name == name).unwrap()
    }

    fn put_thing_body(config: Value) -> String {
        let m = model();
        let input = json!({"Id": "abc", "Token": "tok", "Config": config});
        let (body, ct) = rest_xml_body(op(&m, "PutThing"), &m, &input);
        assert_eq!(ct, Some("application/xml"));
        body.unwrap()
    }

    #[test]
    fn model_is_detected_as_rest_xml() {
        assert!(model().is_rest_xml());
    }

    #[test]
    fn payload_root_uses_member_xml_name_and_service_namespace() {
        let body = put_thing_body(json!({"Name": "n"}));
        assert_eq!(
            body,
            r#"<ThingConfiguration xmlns="https://svc.example/doc/"><Name>n</Name></ThingConfiguration>"#
        );
    }

    #[test]
    fn payload_root_falls_back_to_target_xml_name_then_shape_name() {
        let mut m = model();
        let req = m.shapes.get_mut("t#PutThingRequest").unwrap();
        if let ShapeType::Structure { members } = &mut req.shape_type {
            let config = members.iter_mut().find(|m| m.name == "Config").unwrap();
            config.traits.xml_name = None;
        }
        let member = match &m.shapes["t#PutThingRequest"].shape_type {
            ShapeType::Structure { members } => {
                members.iter().find(|m| m.name == "Config").unwrap().clone()
            }
            _ => unreachable!(),
        };
        assert_eq!(payload_root(&m, &member).0, "ThingConfig");
        m.shapes.get_mut("t#ThingConfig").unwrap().traits.xml_name = Some("Cfg".to_string());
        assert_eq!(payload_root(&m, &member).0, "Cfg");
    }

    #[test]
    fn non_payload_members_stay_out_of_the_body() {
        // Label and header members bind outside the body even when present.
        let body = put_thing_body(json!({"Name": "n"}));
        assert!(!body.contains("abc") && !body.contains("tok"), "{body}");
    }

    #[test]
    fn attributes_go_on_the_open_tag() {
        let body = put_thing_body(json!({"Kind": "a&b", "Name": "n"}));
        assert!(
            body.starts_with(
                r#"<ThingConfiguration xmlns="https://svc.example/doc/" kind="a&amp;b"><Name>"#
            ),
            "{body}"
        );
        assert!(!body.contains("<Kind>"), "{body}");
    }

    #[test]
    fn member_namespace_with_prefix_and_prefixed_attribute() {
        let body = put_thing_body(json!({"Grantee": {"Type": "CanonicalUser", "ID": "x"}}));
        assert!(
            body.contains(
                r#"<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>x</ID></Grantee>"#
            ),
            "{body}"
        );
    }

    #[test]
    fn flattened_list_repeats_member_element() {
        let body = put_thing_body(json!({"Rule": ["r1", "r2"]}));
        assert!(body.contains("<Rule>r1</Rule><Rule>r2</Rule>"), "{body}");
        assert!(!body.contains("<member>"), "{body}");
    }

    #[test]
    fn wrapped_list_uses_list_member_xml_name() {
        let body = put_thing_body(json!({"Aliases": ["a.example", "b.example"]}));
        assert!(
            body.contains("<Aliases><CNAME>a.example</CNAME><CNAME>b.example</CNAME></Aliases>"),
            "{body}"
        );
    }

    #[test]
    fn wrapped_map_uses_entry_and_key_value_names() {
        let body = put_thing_body(json!({"Labels": {"env": "prod"}}));
        assert!(
            body.contains("<Labels><entry><K>env</K><V>prod</V></entry></Labels>"),
            "{body}"
        );
    }

    #[test]
    fn flattened_map_repeats_member_element_without_entry() {
        let body = put_thing_body(json!({"Header": {"a": "1", "b": "2"}}));
        assert!(
            body.contains(
                "<Header><key>a</key><value>1</value></Header><Header><key>b</key><value>2</value></Header>"
            ),
            "{body}"
        );
        assert!(!body.contains("<entry>"), "{body}");
    }

    #[test]
    fn scalars_timestamps_and_blobs() {
        let body = put_thing_body(json!({
            "Enabled": false,
            "Count": 7,
            "Data": "dGVzdA==",
            "At": "2024-01-01T00:00:00Z",
            "AtEpoch": "2024-01-01T00:00:00Z",
            "AtHttp": "2024-01-01T00:00:00Z"
        }));
        for expected in [
            "<Enabled>false</Enabled>",
            "<Count>7</Count>",
            "<Data>dGVzdA==</Data>",
            "<At>2024-01-01T00:00:00Z</At>",
            "<AtEpoch>1704067200</AtEpoch>",
            "<AtHttp>Mon, 01 Jan 2024 00:00:00 GMT</AtHttp>",
        ] {
            assert!(body.contains(expected), "missing {expected} in {body}");
        }
    }

    #[test]
    fn text_is_escaped() {
        let body = put_thing_body(json!({"Name": "<a & 'b'>\r\n"}));
        assert!(
            body.contains("<Name>&lt;a &amp; &apos;b&apos;&gt;&#xD;&#xA;</Name>"),
            "{body}"
        );
    }

    #[test]
    fn omitted_payload_sends_no_body() {
        let m = model();
        let input = json!({"Id": "abc"});
        assert_eq!(rest_xml_body(op(&m, "PutThing"), &m, &input), (None, None));
    }

    #[test]
    fn unbound_members_wrap_in_input_shape_element() {
        let m = model();
        let input = json!({"Id": "w1", "Marker": "m", "Name": "n", "Comment": "c"});
        let (body, ct) = rest_xml_body(op(&m, "CreateWidget"), &m, &input);
        assert_eq!(ct, Some("application/xml"));
        assert_eq!(
            body.unwrap(),
            r#"<CreateWidgetRequest xmlns="https://svc.example/doc/"><Name>n</Name><Comment>c</Comment></CreateWidgetRequest>"#
        );
    }

    #[test]
    fn http_date_and_epoch_conversions() {
        assert_eq!(
            format_timestamp("1970-01-01T00:00:00Z", Some("epoch-seconds")),
            "0"
        );
        assert_eq!(
            format_timestamp("2000-02-29T12:34:56Z", Some("http-date")),
            "Tue, 29 Feb 2000 12:34:56 GMT"
        );
        // Unparseable values reach the wire unchanged.
        assert_eq!(
            format_timestamp("garbage", Some("epoch-seconds")),
            "garbage"
        );
    }

    fn cloudfront_model() -> ServiceModel {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../aws-models/cloudfront.json");
        crate::smithy::parse_model(&path).expect("parse cloudfront model")
    }

    /// Every generated CreateDistribution / CreateDistributionWithTags body
    /// reaches the real CloudFront handler as XML it can deserialize, and the
    /// minimal `required_only` request creates a distribution. Before the
    /// probe serialized restXml, these were JSON and every variant was
    /// rejected as `InvalidArgument` without the handler ever running.
    #[tokio::test]
    async fn create_distribution_probe_bodies_parse_in_cloudfront_handler() {
        use fakecloud_core::service::{AwsRequest, AwsService};

        let model = cloudfront_model();
        for op_name in ["CreateDistribution", "CreateDistributionWithTags"] {
            // A fresh service per operation: both ops' `required_only`
            // variants reuse the same CallerReference.
            let svc = fakecloud_cloudfront::CloudFrontService::new(std::sync::Arc::new(
                parking_lot::RwLock::new(fakecloud_cloudfront::CloudFrontAccounts::new()),
            ));
            let op = op(&model, op_name);
            let variants = crate::generators::generate_all_variants(
                &model,
                op_name,
                &std::collections::HashMap::new(),
            );
            assert!(!variants.is_empty());
            let mut created = false;
            for variant in &variants {
                let (method, path_and_query, headers, body) =
                    super::super::rest_request::build_http_request_from_model(
                        op,
                        &model,
                        &variant.input,
                    )
                    .unwrap();
                let (path, query) = path_and_query
                    .split_once('?')
                    .map(|(p, q)| (p.to_string(), q.to_string()))
                    .unwrap_or((path_and_query.clone(), String::new()));
                let mut header_map = http::HeaderMap::new();
                for (k, v) in &headers {
                    header_map.insert(
                        http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                        http::HeaderValue::from_str(v).unwrap(),
                    );
                }
                let body = body.unwrap_or_default();
                let req = AwsRequest {
                    service: "cloudfront".into(),
                    action: String::new(),
                    region: "us-east-1".into(),
                    account_id: "123456789012".into(),
                    request_id: format!("{op_name}-{}", variant.name),
                    headers: header_map,
                    query_params: std::collections::HashMap::new(),
                    body_stream: parking_lot::Mutex::new(None),
                    body: bytes::Bytes::from(body.clone()),
                    path_segments: fakecloud_core::path::split_path_segments(&path),
                    raw_path: path.clone(),
                    raw_query: query,
                    method: http::Method::from_bytes(method.as_str().as_bytes()).unwrap(),
                    is_query_protocol: false,
                    access_key_id: None,
                    principal: None,
                };
                match svc.handle(req).await {
                    Ok(resp) => {
                        assert_eq!(resp.status.as_u16(), 201, "{op_name} {}", variant.name);
                        created = true;
                    }
                    Err(e) => {
                        // A generated body may still be semantically invalid
                        // (random origin ids, omitted required members), but a
                        // body the probe serialized must never fail to parse
                        // as XML.
                        let msg = e.message();
                        let xml_level = ["Event::", "ill-formed", "syntax", "malformed"]
                            .iter()
                            .any(|needle| msg.contains(needle));
                        assert!(
                            body.is_empty() || !xml_level,
                            "{op_name} {}: body did not parse: {msg}\n{body}",
                            variant.name
                        );
                    }
                }
                if variant.name == "required_only" {
                    let root = if op_name == "CreateDistribution" {
                        "<DistributionConfig xmlns="
                    } else {
                        "<DistributionConfigWithTags xmlns="
                    };
                    assert!(body.starts_with(root), "{body}");
                    // Elements follow the model's declaration order (the
                    // order the CloudFront XSD sequence requires), not the
                    // alphabetical order of the generated JSON object.
                    if op_name == "CreateDistribution" {
                        let first_child = body.split_once('>').unwrap().1;
                        assert!(first_child.starts_with("<CallerReference>"), "{body}");
                    }
                }
            }
            assert!(created, "no {op_name} variant created a distribution");
        }
    }
}
