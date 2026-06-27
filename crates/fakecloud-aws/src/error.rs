use bytes::Bytes;
use http::StatusCode;
use quick_xml::se::Serializer as XmlSerializer;
use serde::Serialize;

/// Build an AWS XML error response (used by Query protocol services: SQS, SNS, IAM, STS).
pub fn xml_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
) -> (StatusCode, String, Bytes) {
    #[derive(Serialize)]
    #[serde(rename = "ErrorResponse")]
    struct ErrorResponse<'a> {
        #[serde(rename = "Error")]
        error: ErrorBody<'a>,
        #[serde(rename = "RequestId")]
        request_id: &'a str,
    }

    #[derive(Serialize)]
    struct ErrorBody<'a> {
        #[serde(rename = "Type")]
        error_type: &'a str,
        #[serde(rename = "Code")]
        code: &'a str,
        #[serde(rename = "Message")]
        message: &'a str,
    }

    let error_type = if status.is_server_error() {
        "Receiver"
    } else {
        "Sender"
    };

    let resp = ErrorResponse {
        error: ErrorBody {
            error_type,
            code,
            message,
        },
        request_id,
    };

    let mut buffer = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    let mut ser = XmlSerializer::new(&mut buffer);
    ser.indent(' ', 2);
    resp.serialize(ser)
        .expect("XML serialization should not fail");

    (status, "text/xml".to_string(), Bytes::from(buffer))
}

/// Build an AWS JSON error response (used by JSON protocol services: SSM, EventBridge, etc.).
pub fn json_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
) -> (StatusCode, String, Bytes) {
    json_error_response_with_fields(status, code, message, &[])
}

/// Build an AWS JSON error response carrying additional top-level members (e.g.
/// DynamoDB's `ConditionalCheckFailedException.Item`). Each field value is
/// parsed as JSON when possible (so an object/array is embedded as-is) and
/// otherwise emitted as a JSON string.
pub fn json_error_response_with_fields(
    status: StatusCode,
    code: &str,
    message: &str,
    extra_fields: &[(String, String)],
) -> (StatusCode, String, Bytes) {
    let mut body = serde_json::Map::new();
    body.insert("__type".to_string(), serde_json::json!(code));
    body.insert("message".to_string(), serde_json::json!(message));
    for (key, value) in extra_fields {
        let parsed = serde_json::from_str(value)
            .unwrap_or_else(|_| serde_json::Value::String(value.clone()));
        body.insert(key.clone(), parsed);
    }

    (
        status,
        "application/x-amz-json-1.1".to_string(),
        Bytes::from(serde_json::Value::Object(body).to_string()),
    )
}

/// Build an S3-style XML error response.
/// S3 uses `<Error>` (not `<ErrorResponse>`) with different field ordering.
pub fn s3_xml_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
) -> (StatusCode, String, Bytes) {
    s3_xml_error_response_with_fields(status, code, message, request_id, &[])
}

/// Build an S3-style XML error response with additional fields (e.g., BucketName, Key).
pub fn s3_xml_error_response_with_fields(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
    extra_fields: &[(String, String)],
) -> (StatusCode, String, Bytes) {
    let mut buffer = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n");
    buffer.push_str(&format!("  <Code>{}</Code>\n", xml_escape(code)));
    buffer.push_str(&format!("  <Message>{}</Message>\n", xml_escape(message)));
    for (key, value) in extra_fields {
        buffer.push_str(&format!("  <{}>{}</{}>\n", key, xml_escape(value), key));
    }
    buffer.push_str(&format!(
        "  <RequestId>{}</RequestId>\n",
        xml_escape(request_id)
    ));
    buffer.push_str("</Error>");

    (status, "application/xml".to_string(), Bytes::from(buffer))
}

use crate::xml::xml_escape;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_error_has_correct_structure() {
        let (status, content_type, body) = xml_error_response(
            StatusCode::BAD_REQUEST,
            "InvalidAction",
            "not found",
            "req-1",
        );
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(content_type, "text/xml");
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert!(body_str.contains("<Code>InvalidAction</Code>"));
        assert!(body_str.contains("<RequestId>req-1</RequestId>"));
    }

    #[test]
    fn json_error_has_correct_structure() {
        let (status, content_type, body) =
            json_error_response(StatusCode::BAD_REQUEST, "ValidationException", "bad input");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(content_type.contains("json"));
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["__type"], "ValidationException");
        assert_eq!(v["message"], "bad input");
    }

    #[test]
    fn s3_xml_error_basic() {
        let (status, content_type, body) =
            s3_xml_error_response(StatusCode::NOT_FOUND, "NoSuchKey", "not found", "req-x");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(content_type, "application/xml");
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert!(body_str.contains("<Code>NoSuchKey</Code>"));
        assert!(body_str.contains("<Message>not found</Message>"));
        assert!(body_str.contains("<RequestId>req-x</RequestId>"));
    }

    #[test]
    fn s3_xml_error_with_fields_includes_extra() {
        let extras = vec![("BucketName".to_string(), "my-bucket".to_string())];
        let (_, _, body) = s3_xml_error_response_with_fields(
            StatusCode::CONFLICT,
            "BucketAlreadyOwnedByYou",
            "already owns",
            "req-1",
            &extras,
        );
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert!(body_str.contains("<BucketName>my-bucket</BucketName>"));
    }

    #[test]
    fn xml_error_escapes_special_chars() {
        let (_, _, body) = xml_error_response(StatusCode::BAD_REQUEST, "E", "a<b>c", "req-1");
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert!(body_str.contains("a&lt;b&gt;c"));
    }
}
