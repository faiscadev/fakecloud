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

/// Lambda's modeled errors whose message member is `message`; every other
/// modeled Lambda error names it `Message`. Matches `aws-models/lambda.json`
/// (checked by a unit test).
const LAMBDA_LOWERCASE_MESSAGE_ERRORS: &[&str] = &[
    "AliasLimitExceededException",
    "CapacityProviderLimitExceededException",
    "CodeArtifactUserDeletedException",
    "CodeArtifactUserFailedException",
    "CodeArtifactUserPendingException",
    "CodeStorageExceededException",
    "FunctionVersionsPerCapacityProviderLimitExceededException",
    "InvalidParameterValueException",
    "InvalidRequestContentException",
    "ModeNotSupportedException",
    "PolicyLengthExceededException",
    "PreconditionFailedException",
    "ProvisionedConcurrencyConfigNotFoundException",
    "RequestTooLargeException",
    "ResourceConflictException",
    "ResourceNotReadyException",
    "SerializedRequestEntityTooLargeException",
    "TooManyRequestsException",
    "UnsupportedMediaTypeException",
];

/// Lambda's modeled errors whose message member is `Message`. Matches
/// `aws-models/lambda.json`.
const LAMBDA_MODELED_ERRORS_WITH_CAPITAL_MESSAGE: &[&str] = &[
    "CallbackTimeoutException",
    "CodeSigningConfigNotFoundException",
    "CodeVerificationFailedException",
    "DurableExecutionAlreadyStartedException",
    "EC2AccessDeniedException",
    "EC2ThrottledException",
    "EC2UnexpectedException",
    "EFSIOException",
    "EFSMountConnectivityException",
    "EFSMountFailureException",
    "EFSMountTimeoutException",
    "ENILimitReachedException",
    "ENINotReadyException",
    "InvalidCodeSignatureException",
    "InvalidRuntimeException",
    "InvalidSecurityGroupIDException",
    "InvalidSubnetIDException",
    "InvalidZipFileException",
    "KMSAccessDeniedException",
    "KMSDisabledException",
    "KMSInvalidStateException",
    "KMSNotFoundException",
    "NoPublishedVersionException",
    "PublicPolicyException",
    "RecursiveInvocationException",
    "ResourceInUseException",
    "ResourceNotFoundException",
    "S3FilesMountConnectivityException",
    "S3FilesMountFailureException",
    "S3FilesMountTimeoutException",
    "ServiceException",
    "ServiceQuotaExceededException",
    "SnapStartException",
    "SnapStartNotReadyException",
    "SnapStartRegenerationFailureException",
    "SnapStartTimeoutException",
    "SubnetIPAddressLimitReachedException",
];

/// Modeled Lambda errors that AWS sends without a `Type` member, though the
/// model declares one: every recording of them is `{"Message": ...}` alone
/// (`tests/fixtures/durable/aws-round3-kms-*.jsonl` in fakecloud-lambda).
const LAMBDA_ERRORS_WITHOUT_TYPE: &[&str] = &["KMSDisabledException", "KMSInvalidStateException"];

/// Modeled Lambda errors that AWS sends with `"Type": null` on every recorded
/// path (`DurableExecutionAlreadyStartedException`, aws-round1/2). Other
/// paths that send `null` for an error usually typed `User` (an undecodable
/// callback id's `InvalidParameterValueException`) set it at the raise site
/// with a `Type` extra field.
const LAMBDA_ERRORS_WITH_NULL_TYPE: &[&str] = &["DurableExecutionAlreadyStartedException"];

/// Errors outside Lambda's model that AWS sends as `{"Message": ...}`
/// (`AccessDeniedException`, recorded in aws-round3/4); the rest
/// (`ValidationException`, `UnrecognizedClientException`, ...) are
/// `{"message": ...}`.
const LAMBDA_UNMODELED_CAPITAL_MESSAGE_ERRORS: &[&str] = &["AccessDeniedException"];

/// The body of a Lambda error, as recorded against Lambda (us-east-1). A
/// modeled error carries its members by their Smithy names: `Message` or
/// `message` as the shape names it, and `Type` (`User`; `Service` for
/// `ServiceException`; `null` or absent for the errors listed above), e.g.
/// `{"Type":"User","Message":"Function not found: ..."}`,
/// `{"Type":"User","message":"Invalid marker value: ..."}`. An error outside
/// Lambda's model is `{"message": ...}` (`ValidationException`,
/// `UnrecognizedClientException`), or `{"Message": ...}` for
/// `AccessDeniedException`. The code itself travels in `x-amzn-ErrorType`.
/// `extra_fields` are added as further members after these and replace them
/// on a clash (`Reason`, `EC2ErrorCode`, a raise site's `"Type": null`).
pub fn lambda_json_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    extra_fields: &[(String, String)],
) -> (StatusCode, String, Bytes) {
    let mut body = serde_json::Map::new();
    let lowercase = LAMBDA_LOWERCASE_MESSAGE_ERRORS.contains(&code);
    if lowercase || LAMBDA_MODELED_ERRORS_WITH_CAPITAL_MESSAGE.contains(&code) {
        if LAMBDA_ERRORS_WITH_NULL_TYPE.contains(&code) {
            body.insert("Type".to_string(), serde_json::Value::Null);
        } else if !LAMBDA_ERRORS_WITHOUT_TYPE.contains(&code) {
            let kind = if code == "ServiceException" {
                "Service"
            } else {
                "User"
            };
            body.insert("Type".to_string(), serde_json::json!(kind));
        }
        let member = if lowercase { "message" } else { "Message" };
        body.insert(member.to_string(), serde_json::json!(message));
    } else if LAMBDA_UNMODELED_CAPITAL_MESSAGE_ERRORS.contains(&code) {
        body.insert("Message".to_string(), serde_json::json!(message));
    } else {
        body.insert("message".to_string(), serde_json::json!(message));
    }
    for (key, value) in extra_fields {
        let parsed = serde_json::from_str(value)
            .unwrap_or_else(|_| serde_json::Value::String(value.clone()));
        body.insert(key.clone(), parsed);
    }
    (
        status,
        "application/json".to_string(),
        Bytes::from(serde_json::Value::Object(body).to_string()),
    )
}

/// Whether a REST-JSON service's errors use Lambda's body shape
/// ([`lambda_json_error_response`]).
pub fn uses_lambda_error_body(service: &str) -> bool {
    service == "lambda"
}

/// XML namespace of the CloudFront API (`2020-05-31`).
pub const CLOUDFRONT_XMLNS: &str = "http://cloudfront.amazonaws.com/doc/2020-05-31/";

/// XML namespace of the Route 53 API (`2013-04-01`).
pub const ROUTE53_XMLNS: &str = "https://route53.amazonaws.com/doc/2013-04-01/";

/// The namespace of a REST-XML service whose errors use the `<ErrorResponse>`
/// wrapper (see [`rest_xml_error_response`]), or `None` for one that answers
/// with S3's bare `<Error>` document.
pub fn rest_xml_error_namespace(service: &str) -> Option<&'static str> {
    match service {
        "cloudfront" => Some(CLOUDFRONT_XMLNS),
        "route53" => Some(ROUTE53_XMLNS),
        _ => None,
    }
}

/// Build a REST-XML error response in the `<ErrorResponse>` shape CloudFront
/// and Route 53 use:
///
/// ```xml
/// <ErrorResponse xmlns="{namespace}">
///   <Error><Type>Sender</Type><Code>..</Code><Message>..</Message></Error>
///   <RequestId>..</RequestId>
/// </ErrorResponse>
/// ```
///
/// `Type` is `Receiver` for a 5xx and `Sender` otherwise. Unlike S3's bare
/// `<Error>`, the AWS SDKs for these services only find the code inside this
/// wrapper.
pub fn rest_xml_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
    namespace: &str,
) -> (StatusCode, String, Bytes) {
    let error_type = if status.is_server_error() {
        "Receiver"
    } else {
        "Sender"
    };
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <ErrorResponse xmlns=\"{}\"><Error><Type>{error_type}</Type><Code>{}</Code>\
         <Message>{}</Message></Error><RequestId>{}</RequestId></ErrorResponse>",
        xml_escape(namespace),
        xml_escape(code),
        xml_escape(message),
        xml_escape(request_id),
    );
    (status, "text/xml".to_string(), Bytes::from(body))
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

/// Build an S3 Control XML error response. S3 Control is served by the S3
/// handler but, unlike S3's bare `<Error>`, wraps its errors in an
/// un-namespaced `<ErrorResponse>`; the S3 Control SDKs only find the code
/// inside that wrapper.
///
/// ```xml
/// <ErrorResponse>
///   <Error><Code>..</Code><Message>..</Message>..</Error>
///   <RequestId>..</RequestId>
/// </ErrorResponse>
/// ```
pub fn s3_control_xml_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
    extra_fields: &[(String, String)],
) -> (StatusCode, String, Bytes) {
    let mut buffer =
        String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ErrorResponse><Error>");
    buffer.push_str(&format!("<Code>{}</Code>", xml_escape(code)));
    buffer.push_str(&format!("<Message>{}</Message>", xml_escape(message)));
    for (key, value) in extra_fields {
        buffer.push_str(&format!("<{}>{}</{}>", key, xml_escape(value), key));
    }
    buffer.push_str(&format!(
        "</Error><RequestId>{}</RequestId></ErrorResponse>",
        xml_escape(request_id)
    ));
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
    fn s3_control_xml_error_is_wrapped() {
        let (status, _, body) = s3_control_xml_error_response(
            StatusCode::NOT_FOUND,
            "NoSuchBucket",
            "The specified bucket does not exist",
            "req-c",
            &[("BucketName".to_string(), "b<1>".to_string())],
        );
        assert_eq!(status, StatusCode::NOT_FOUND);
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            body.contains(
                "<ErrorResponse><Error><Code>NoSuchBucket</Code>\
                 <Message>The specified bucket does not exist</Message>\
                 <BucketName>b&lt;1&gt;</BucketName></Error>\
                 <RequestId>req-c</RequestId></ErrorResponse>"
            ),
            "{body}"
        );
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
    fn rest_xml_error_wraps_in_namespaced_error_response() {
        let (status, content_type, body) = rest_xml_error_response(
            StatusCode::NOT_FOUND,
            "NoSuchDistribution",
            "The specified distribution does not exist.",
            "req-cf",
            CLOUDFRONT_XMLNS,
        );
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(content_type, "text/xml");
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assert!(body.contains(
            "<ErrorResponse xmlns=\"http://cloudfront.amazonaws.com/doc/2020-05-31/\">\
             <Error><Type>Sender</Type><Code>NoSuchDistribution</Code>\
             <Message>The specified distribution does not exist.</Message></Error>\
             <RequestId>req-cf</RequestId></ErrorResponse>"
        ));
    }

    #[test]
    fn rest_xml_error_type_is_receiver_for_5xx() {
        let (_, _, body) = rest_xml_error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "boom",
            "r",
            ROUTE53_XMLNS,
        );
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("<Type>Receiver</Type>"), "{body}");
        assert!(body.contains("xmlns=\"https://route53.amazonaws.com/doc/2013-04-01/\""));
    }

    #[test]
    fn rest_xml_error_escapes_code_message_and_request_id() {
        let (_, _, body) =
            rest_xml_error_response(StatusCode::BAD_REQUEST, "A&B", "x<y>", "r\"1", "urn:ns");
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("<Code>A&amp;B</Code>"), "{body}");
        assert!(body.contains("<Message>x&lt;y&gt;</Message>"), "{body}");
        assert!(body.contains("<RequestId>r&quot;1</RequestId>"), "{body}");
    }

    #[test]
    fn rest_xml_error_namespace_only_for_wrapped_services() {
        assert_eq!(
            rest_xml_error_namespace("cloudfront"),
            Some(CLOUDFRONT_XMLNS)
        );
        assert_eq!(rest_xml_error_namespace("route53"), Some(ROUTE53_XMLNS));
        assert_eq!(rest_xml_error_namespace("s3"), None);
        assert_eq!(rest_xml_error_namespace("ecr"), None);
    }

    #[test]
    fn xml_error_escapes_special_chars() {
        let (_, _, body) = xml_error_response(StatusCode::BAD_REQUEST, "E", "a<b>c", "req-1");
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert!(body_str.contains("a&lt;b&gt;c"));
    }

    /// Lambda's error bodies follow `aws-models/lambda.json`: every modeled
    /// error is listed with the message member its shape names.
    #[test]
    fn lambda_error_tables_match_the_model() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../aws-models/lambda.json");
        let model: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let mut lower = Vec::new();
        let mut upper = Vec::new();
        for (id, shape) in model["shapes"].as_object().unwrap() {
            if shape["traits"].get("smithy.api#error").is_none() {
                continue;
            }
            let name = id.rsplit('#').next().unwrap().to_string();
            let members = shape["members"].as_object().unwrap();
            assert!(members.contains_key("Type"), "{name}");
            if members.contains_key("message") {
                lower.push(name);
            } else {
                assert!(members.contains_key("Message"), "{name}");
                upper.push(name);
            }
        }
        lower.sort();
        upper.sort();
        assert_eq!(LAMBDA_LOWERCASE_MESSAGE_ERRORS, lower.as_slice());
        assert_eq!(LAMBDA_MODELED_ERRORS_WITH_CAPITAL_MESSAGE, upper.as_slice());
    }

    #[test]
    fn lambda_error_bodies_match_the_recordings() {
        let body = |code: &str, message: &str| -> serde_json::Value {
            let (_, content_type, bytes) =
                lambda_json_error_response(StatusCode::BAD_REQUEST, code, message, &[]);
            assert_eq!(content_type, "application/json");
            serde_json::from_slice(&bytes).unwrap()
        };
        assert_eq!(
            body("ResourceNotFoundException", "Function not found: f"),
            serde_json::json!({"Type": "User", "Message": "Function not found: f"})
        );
        assert_eq!(
            body(
                "InvalidParameterValueException",
                "Invalid marker value: garbage"
            ),
            serde_json::json!({"Type": "User", "message": "Invalid marker value: garbage"})
        );
        assert_eq!(
            body("ValidationException", "1 validation error detected"),
            serde_json::json!({"message": "1 validation error detected"})
        );
        assert_eq!(
            body("ServiceException", "boom")["Type"],
            serde_json::json!("Service")
        );
    }

    /// Every Lambda error response recorded against AWS in fakecloud-lambda's
    /// fixtures is reproduced byte-for-byte in meaning (same JSON members and
    /// values) from its `x-amzn-ErrorType` code and message. The undecodable
    /// callback id answers carry their `"Type": null` from the raise site.
    #[test]
    fn lambda_error_bodies_reproduce_every_recording() {
        let dir = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../fakecloud-lambda/tests/fixtures"
        );
        let mut files = Vec::new();
        let mut stack = vec![std::path::PathBuf::from(dir)];
        while let Some(d) = stack.pop() {
            for entry in std::fs::read_dir(&d).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|e| e == "jsonl") {
                    files.push(path);
                }
            }
        }
        let mut checked = 0;
        for file in files {
            for line in std::fs::read_to_string(&file).unwrap().lines() {
                let Ok(record) = serde_json::from_str::<serde_json::Value>(line) else {
                    continue;
                };
                let (Some(status), Some(code), Some(body)) = (
                    record["status"].as_u64(),
                    record["headers"]["x-amzn-errortype"].as_str(),
                    record["body"].as_str(),
                ) else {
                    continue;
                };
                if status < 400 {
                    continue;
                }
                let code = code.split(':').next().unwrap();
                let recorded: serde_json::Value = serde_json::from_str(body).unwrap();
                let message = recorded
                    .get("Message")
                    .or_else(|| recorded.get("message"))
                    .and_then(|m| m.as_str())
                    .unwrap();
                let label = record["label"].as_str().unwrap_or_default();
                let extra: Vec<(String, String)> =
                    if label.starts_with("callback-") && label.ends_with("-unknown") {
                        vec![("Type".to_string(), "null".to_string())]
                    } else {
                        Vec::new()
                    };
                let (_, _, ours) = lambda_json_error_response(
                    StatusCode::from_u16(status as u16).unwrap(),
                    code,
                    message,
                    &extra,
                );
                let ours: serde_json::Value = serde_json::from_slice(&ours).unwrap();
                assert_eq!(ours, recorded, "{label} in {}", file.display());
                checked += 1;
            }
        }
        assert!(checked >= 70, "only {checked} recorded errors found");
    }
}
