//! Serialization helpers for the AWS `ec2Query` protocol.
//!
//! EC2 uses a variant of the AWS Query protocol whose XML responses differ
//! from `awsQuery` (SQS/SNS/RDS/IAM) in three load-bearing ways:
//!
//! 1. **No `<Result>` wrapper.** An `awsQuery` response nests the body under
//!    `<{Action}Result>`. EC2 places the result members directly inside
//!    `<{Action}Response>`, preceded by a lowercase `<requestId>` element
//!    (NOT the `<ResponseMetadata><RequestId>` block awsQuery uses).
//! 2. **Flattened lists.** Lists serialize as a wrapper element whose repeated
//!    children are literally named `<item>` — e.g.
//!    `<instancesSet><item>…</item><item>…</item></instancesSet>` — with no
//!    intermediate `<member>` tag.
//! 3. **lowerCamelCase tags.** Response element names are lower-camel
//!    (`<reservationId>`, `<tagSet>`, `<groupSet>`), unlike the UpperCamel
//!    member names awsQuery echoes.
//!
//! The real AWS SDKs deserialize strictly against these shapes, so the unit
//! tests below round-trip the rendered XML through a minimal parser to lock the
//! structure in. See [`crate::xml::xml_escape`] for content escaping.

use bytes::Bytes;
use http::StatusCode;

use crate::xml::xml_escape;

/// Build an `ec2Query` error response.
///
/// EC2's error envelope differs from awsQuery's `<ErrorResponse>` shape in
/// three load-bearing ways the AWS SDKs deserialize strictly against:
///
/// ```xml
/// <?xml version="1.0" encoding="UTF-8"?>
/// <Response>
///   <Errors>
///     <Error>
///       <Code>InvalidInstanceID.NotFound</Code>
///       <Message>The instance ID 'i-123' does not exist</Message>
///     </Error>
///   </Errors>
///   <RequestID>{request_id}</RequestID>
/// </Response>
/// ```
///
/// 1. The root element is `<Response>` (not `<ErrorResponse>`).
/// 2. Errors nest inside an `<Errors>` wrapper, and each `<Error>` carries only
///    `<Code>` + `<Message>` — there is NO `<Type>` element (awsQuery emits one).
/// 3. The request id element is `<RequestID>` (capital I-D), not `<RequestId>`.
///
/// aws-sdk-go-v2 (Terraform), aws-sdk-js v3, aws-sdk-rust, and aws-sdk-java v2
/// read the error code via `Response > Errors > Error > Code` and the id via
/// `RequestID`; the awsQuery `<ErrorResponse>` shape leaves both empty for them,
/// breaking `InvalidInstanceID.NotFound` / `DependencyViolation` branching and
/// request-id-based retries. Only botocore/CLI are lenient enough to recover the
/// code from the wrong shape.
pub fn ec2_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
) -> (StatusCode, String, Bytes) {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <Response><Errors><Error><Code>{code}</Code><Message>{message}</Message></Error></Errors>\
         <RequestID>{rid}</RequestID></Response>",
        code = xml_escape(code),
        message = xml_escape(message),
        rid = xml_escape(request_id),
    );
    (status, "text/xml".to_string(), Bytes::from(body))
}

/// The EC2 response namespace. Note the trailing slash — EC2 emits it on the
/// wire even though the Smithy model's `xmlNamespace` uri omits it.
pub const EC2_NAMESPACE: &str = "http://ec2.amazonaws.com/doc/2016-11-15/";

/// Wrap an operation's result `body` in the ec2Query response envelope.
///
/// ```xml
/// <?xml version="1.0" encoding="UTF-8"?>
/// <RunInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
///   <requestId>{request_id}</requestId>
///   {body}
/// </RunInstancesResponse>
/// ```
///
/// `body` is the already-rendered, already-escaped inner XML (use the element
/// and list helpers in this module to build it).
pub fn ec2_response(action: &str, request_id: &str, body: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <{action}Response xmlns=\"{EC2_NAMESPACE}\">\
         <requestId>{rid}</requestId>{body}</{action}Response>",
        rid = xml_escape(request_id),
    )
}

/// Render a leaf element with escaped text content: `<name>value</name>`.
pub fn ec2_elem(name: &str, value: &str) -> String {
    format!("<{name}>{}</{name}>", xml_escape(value))
}

/// Render an optional leaf element. Returns the empty string when `value` is
/// `None` (EC2 omits absent optional members rather than emitting empty tags).
pub fn ec2_elem_opt(name: &str, value: Option<&str>) -> String {
    match value {
        Some(v) => ec2_elem(name, v),
        None => String::new(),
    }
}

/// Render a boolean as EC2's lowercase `true`/`false`: `<name>true</name>`.
pub fn ec2_bool(name: &str, value: bool) -> String {
    format!("<{name}>{value}</{name}>")
}

/// Render the canonical `<return>true</return>` body emitted by EC2's many
/// mutate-and-acknowledge operations (DeleteVpc, AuthorizeSecurityGroupIngress,
/// …). Pass to [`ec2_response`] as the body.
pub fn ec2_return(value: bool) -> String {
    format!("<return>{value}</return>")
}

/// Render a flattened EC2 list. `wrapper` is the set element name
/// (e.g. `instancesSet`); each entry in `items` is the already-rendered inner
/// XML of one `<item>` (do NOT pre-wrap entries in `<item>`).
///
/// An empty list still emits the wrapper element (`<wrapper/>`-equivalent),
/// matching how the AWS SDKs expect an empty set to deserialize to `[]`.
pub fn ec2_list(wrapper: &str, items: &[String]) -> String {
    if items.is_empty() {
        return format!("<{wrapper}/>");
    }
    let mut out = String::with_capacity(items.len() * 16 + wrapper.len() * 2 + 5);
    out.push('<');
    out.push_str(wrapper);
    out.push('>');
    for item in items {
        out.push_str("<item>");
        out.push_str(item);
        out.push_str("</item>");
    }
    out.push_str("</");
    out.push_str(wrapper);
    out.push('>');
    out
}

/// Render a flattened list of scalar leaf values, e.g. a `<groupSet>` of bare
/// strings: `<groupSet><item>sg-1</item><item>sg-2</item></groupSet>`.
pub fn ec2_scalar_list(wrapper: &str, values: &[String]) -> String {
    let items: Vec<String> = values.iter().map(|v| xml_escape(v)).collect();
    ec2_list(wrapper, &items)
}

/// Render an EC2 `<tagSet>` from `(key, value)` pairs. Each tag becomes
/// `<item><key>…</key><value>…</value></item>`. Emits `<tagSet/>` when empty.
pub fn ec2_tag_set(tags: &[(String, String)]) -> String {
    let items: Vec<String> = tags
        .iter()
        .map(|(k, v)| format!("{}{}", ec2_elem("key", k), ec2_elem("value", v)))
        .collect();
    ec2_list("tagSet", &items)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal, dependency-free check that a rendered fragment contains a
    /// well-formed, correctly-nested element. Good enough to assert the
    /// structural invariants the AWS SDK deserializer relies on.
    fn contains_balanced(haystack: &str, tag: &str) -> bool {
        haystack.contains(&format!("<{tag}>")) && haystack.contains(&format!("</{tag}>"))
    }

    #[test]
    fn response_envelope_has_no_result_wrapper_and_lowercase_request_id() {
        let xml = ec2_response("DeleteVpc", "req-123", &ec2_return(true));
        assert!(xml.starts_with("<?xml"));
        assert!(
            xml.contains("<DeleteVpcResponse xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\">")
        );
        // ec2Query: lowercase <requestId>, NOT <ResponseMetadata><RequestId>.
        assert!(xml.contains("<requestId>req-123</requestId>"));
        assert!(!xml.contains("ResponseMetadata"));
        assert!(!xml.contains("Result>"));
        assert!(xml.contains("<return>true</return>"));
        assert!(xml.ends_with("</DeleteVpcResponse>"));
    }

    #[test]
    fn list_flattens_with_item_children_no_member_wrapper() {
        let items = vec![
            "<instanceId>i-1</instanceId>".to_string(),
            "<instanceId>i-2</instanceId>".to_string(),
        ];
        let xml = ec2_list("instancesSet", &items);
        assert_eq!(
            xml,
            "<instancesSet><item><instanceId>i-1</instanceId></item>\
             <item><instanceId>i-2</instanceId></item></instancesSet>"
        );
        assert!(!xml.contains("<member>"));
    }

    #[test]
    fn empty_list_emits_self_closing_wrapper() {
        assert_eq!(ec2_list("instancesSet", &[]), "<instancesSet/>");
        assert_eq!(ec2_scalar_list("groupSet", &[]), "<groupSet/>");
        assert_eq!(ec2_tag_set(&[]), "<tagSet/>");
    }

    #[test]
    fn scalar_list_escapes_and_wraps_each_value() {
        let xml = ec2_scalar_list("groupSet", &["sg-1".to_string(), "a&b".to_string()]);
        assert_eq!(
            xml,
            "<groupSet><item>sg-1</item><item>a&amp;b</item></groupSet>"
        );
    }

    #[test]
    fn tag_set_renders_key_value_items() {
        let tags = vec![
            ("Name".to_string(), "web".to_string()),
            ("env".to_string(), "prod & test".to_string()),
        ];
        let xml = ec2_tag_set(&tags);
        assert!(contains_balanced(&xml, "tagSet"));
        assert!(xml.contains("<item><key>Name</key><value>web</value></item>"));
        // content is escaped
        assert!(xml.contains("<value>prod &amp; test</value>"));
    }

    #[test]
    fn elem_helpers_escape_and_omit() {
        assert_eq!(
            ec2_elem("imageId", "ami-<1>"),
            "<imageId>ami-&lt;1&gt;</imageId>"
        );
        assert_eq!(ec2_elem_opt("publicIp", None), "");
        assert_eq!(
            ec2_elem_opt("publicIp", Some("1.2.3.4")),
            "<publicIp>1.2.3.4</publicIp>"
        );
        assert_eq!(
            ec2_bool("ebsOptimized", false),
            "<ebsOptimized>false</ebsOptimized>"
        );
    }

    #[test]
    fn error_response_uses_ec2query_envelope_not_awsquery() {
        let (status, content_type, body) = ec2_error_response(
            StatusCode::BAD_REQUEST,
            "InvalidInstanceID.NotFound",
            "The instance ID 'i-123' does not exist",
            "req-42",
        );
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(content_type, "text/xml");
        let xml = String::from_utf8(body.to_vec()).unwrap();
        // Root <Response>, NOT awsQuery's <ErrorResponse>.
        assert!(xml.contains("<Response>"), "{xml}");
        assert!(!xml.contains("<ErrorResponse>"), "{xml}");
        // <Errors> wrapper around each <Error>.
        assert!(xml.contains("<Errors><Error>"), "{xml}");
        // Code + Message, but NO <Type> element (awsQuery emits Sender/Receiver).
        assert!(
            xml.contains("<Code>InvalidInstanceID.NotFound</Code>"),
            "{xml}"
        );
        assert!(
            xml.contains("<Message>The instance ID &apos;i-123&apos; does not exist</Message>")
                || xml.contains("<Message>The instance ID 'i-123' does not exist</Message>"),
            "{xml}"
        );
        assert!(!xml.contains("<Type>"), "{xml}");
        // Capital-ID <RequestID>, not <RequestId>.
        assert!(xml.contains("<RequestID>req-42</RequestID>"), "{xml}");
        assert!(!xml.contains("<RequestId>"), "{xml}");
    }

    #[test]
    fn error_response_escapes_special_chars_in_message() {
        let (_, _, body) = ec2_error_response(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            "a<b>&c",
            "r-1",
        );
        let xml = String::from_utf8(body.to_vec()).unwrap();
        assert!(xml.contains("<Message>a&lt;b&gt;&amp;c</Message>"), "{xml}");
    }

    #[test]
    fn nested_reservation_shape_round_trips_structurally() {
        // The deepest real shape: reservationSet > item > instancesSet > item.
        let instance = format!(
            "{}{}{}",
            ec2_elem("instanceId", "i-abc"),
            ec2_elem("imageId", "ami-1"),
            ec2_tag_set(&[("Name".to_string(), "x".to_string())]),
        );
        let instances = ec2_list("instancesSet", &[instance]);
        let reservation = format!("{}{}", ec2_elem("reservationId", "r-1"), instances);
        let body = ec2_list("reservationSet", &[reservation]);
        let xml = ec2_response("DescribeInstances", "r-9", &body);
        assert!(contains_balanced(&xml, "reservationSet"));
        assert!(contains_balanced(&xml, "instancesSet"));
        assert!(xml.contains("<reservationId>r-1</reservationId>"));
        assert!(xml.contains("<instanceId>i-abc</instanceId>"));
        // three <item> opens: 1 reservation + 1 instance + 1 tag
        assert_eq!(xml.matches("<item>").count(), 3);
    }
}
