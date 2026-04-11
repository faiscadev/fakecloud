use bytes::Bytes;
use http::HeaderMap;
use std::collections::HashMap;

/// The wire protocol used by an AWS service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwsProtocol {
    /// Query protocol: form-encoded body, Action param, XML response.
    /// Used by: SQS, SNS, IAM, STS.
    Query,
    /// JSON protocol: JSON body, X-Amz-Target header, JSON response.
    /// Used by: SSM, EventBridge, DynamoDB, SecretsManager, KMS, CloudWatch Logs.
    Json,
    /// REST protocol: HTTP method + path-based routing, XML responses.
    /// Used by: S3, API Gateway, Route53.
    Rest,
    /// REST-JSON protocol: HTTP method + path-based routing, JSON responses.
    /// Used by: Lambda, SES v2.
    RestJson,
}

/// Services that use REST protocol with XML responses (detected from SigV4 credential scope).
const REST_XML_SERVICES: &[&str] = &["s3"];

/// Services that use REST protocol with JSON responses (detected from SigV4 credential scope).
const REST_JSON_SERVICES: &[&str] = &["lambda", "ses", "apigateway"];

/// Detected service name and action from an incoming HTTP request.
#[derive(Debug)]
pub struct DetectedRequest {
    pub service: String,
    pub action: String,
    pub protocol: AwsProtocol,
}

/// Detect the target service and action from HTTP request components.
pub fn detect_service(
    headers: &HeaderMap,
    query_params: &HashMap<String, String>,
    body: &Bytes,
) -> Option<DetectedRequest> {
    // 1. Check X-Amz-Target header (JSON protocol)
    if let Some(target) = headers.get("x-amz-target").and_then(|v| v.to_str().ok()) {
        return parse_amz_target(target);
    }

    // 2. Check for Query protocol (Action parameter in query string or form body)
    if let Some(action) = query_params.get("Action") {
        let service =
            extract_service_from_auth(headers).or_else(|| infer_service_from_action(action));
        if let Some(service) = service {
            return Some(DetectedRequest {
                service,
                action: action.clone(),
                protocol: AwsProtocol::Query,
            });
        }
    }

    // 3. Try form-encoded body
    {
        let form_params = decode_form_urlencoded(body);

        if let Some(action) = form_params.get("Action") {
            let service =
                extract_service_from_auth(headers).or_else(|| infer_service_from_action(action));
            if let Some(service) = service {
                return Some(DetectedRequest {
                    service,
                    action: action.clone(),
                    protocol: AwsProtocol::Query,
                });
            }
        }
    }

    // 4. Fallback: check auth header for REST-style services (S3, Lambda, SES, etc.)
    if let Some(service) = extract_service_from_auth(headers) {
        if let Some(protocol) = rest_protocol_for(&service) {
            return Some(DetectedRequest {
                service,
                action: String::new(), // REST services determine action from method+path
                protocol,
            });
        }
    }

    // 5. Check query params for presigned URL auth (X-Amz-Credential for SigV4)
    if let Some(credential) = query_params.get("X-Amz-Credential") {
        // Format: AKID/date/region/service/aws4_request
        let parts: Vec<&str> = credential.split('/').collect();
        if parts.len() >= 4 {
            let service = parts[3].to_string();
            if let Some(protocol) = rest_protocol_for(&service) {
                return Some(DetectedRequest {
                    service,
                    action: String::new(),
                    protocol,
                });
            }
        }
    }

    // 6. Check for SigV2-style presigned URL (AWSAccessKeyId + Signature + Expires)
    //    Only match when all three SigV2 presigned-URL parameters are present so
    //    we don't accidentally claim non-S3 requests.
    if query_params.contains_key("AWSAccessKeyId")
        && query_params.contains_key("Signature")
        && query_params.contains_key("Expires")
    {
        return Some(DetectedRequest {
            service: "s3".to_string(),
            action: String::new(),
            protocol: AwsProtocol::Rest,
        });
    }

    None
}

/// Parse `X-Amz-Target: AWSEvents.PutEvents` -> service=events, action=PutEvents
/// Parse `X-Amz-Target: AmazonSSM.GetParameter` -> service=ssm, action=GetParameter
fn parse_amz_target(target: &str) -> Option<DetectedRequest> {
    let (prefix, action) = target.rsplit_once('.')?;

    let service = match prefix {
        "AWSEvents" => "events",
        "AmazonSSM" => "ssm",
        "AmazonSQS" => "sqs",
        "AmazonSNS" => "sns",
        "DynamoDB_20120810" => "dynamodb",
        "Logs_20140328" => "logs",
        s if s.starts_with("secretsmanager") => "secretsmanager",
        s if s.starts_with("TrentService") => "kms",
        s if s.starts_with("AWSCognitoIdentityProviderService") => "cognito-idp",
        s if s.starts_with("Kinesis_20131202") => "kinesis",
        s if s.starts_with("AWSStepFunctions") => "states",
        _ => return None,
    };

    Some(DetectedRequest {
        service: service.to_string(),
        action: action.to_string(),
        protocol: AwsProtocol::Json,
    })
}

/// Returns the REST protocol variant for a service, or None if not a REST service.
fn rest_protocol_for(service: &str) -> Option<AwsProtocol> {
    if REST_XML_SERVICES.contains(&service) {
        Some(AwsProtocol::Rest)
    } else if REST_JSON_SERVICES.contains(&service) {
        Some(AwsProtocol::RestJson)
    } else {
        None
    }
}

/// Infer service from the action name when no SigV4 auth is present.
/// Some AWS operations (e.g., AssumeRoleWithSAML, AssumeRoleWithWebIdentity)
/// do not require authentication and won't have an Authorization header.
fn infer_service_from_action(action: &str) -> Option<String> {
    match action {
        "AssumeRole"
        | "AssumeRoleWithSAML"
        | "AssumeRoleWithWebIdentity"
        | "GetCallerIdentity"
        | "GetSessionToken"
        | "GetFederationToken"
        | "GetAccessKeyInfo"
        | "DecodeAuthorizationMessage" => Some("sts".to_string()),
        "CreateUser" | "DeleteUser" | "GetUser" | "ListUsers" | "CreateRole" | "DeleteRole"
        | "GetRole" | "ListRoles" | "CreatePolicy" | "DeletePolicy" | "GetPolicy"
        | "ListPolicies" | "AttachRolePolicy" | "DetachRolePolicy" | "CreateAccessKey"
        | "DeleteAccessKey" | "ListAccessKeys" | "ListRolePolicies" => Some("iam".to_string()),
        // SES v1 (Query protocol)
        "VerifyEmailIdentity"
        | "VerifyDomainIdentity"
        | "VerifyDomainDkim"
        | "ListIdentities"
        | "GetIdentityVerificationAttributes"
        | "GetIdentityDkimAttributes"
        | "DeleteIdentity"
        | "SetIdentityDkimEnabled"
        | "SetIdentityNotificationTopic"
        | "SetIdentityFeedbackForwardingEnabled"
        | "GetIdentityNotificationAttributes"
        | "GetIdentityMailFromDomainAttributes"
        | "SetIdentityMailFromDomain"
        | "SendEmail"
        | "SendRawEmail"
        | "SendTemplatedEmail"
        | "SendBulkTemplatedEmail"
        | "CreateTemplate"
        | "GetTemplate"
        | "ListTemplates"
        | "DeleteTemplate"
        | "UpdateTemplate"
        | "CreateConfigurationSet"
        | "DeleteConfigurationSet"
        | "DescribeConfigurationSet"
        | "ListConfigurationSets"
        | "CreateConfigurationSetEventDestination"
        | "UpdateConfigurationSetEventDestination"
        | "DeleteConfigurationSetEventDestination"
        | "GetSendQuota"
        | "GetSendStatistics"
        | "GetAccountSendingEnabled"
        | "CreateReceiptRuleSet"
        | "DeleteReceiptRuleSet"
        | "DescribeReceiptRuleSet"
        | "ListReceiptRuleSets"
        | "CloneReceiptRuleSet"
        | "SetActiveReceiptRuleSet"
        | "ReorderReceiptRuleSet"
        | "CreateReceiptRule"
        | "DeleteReceiptRule"
        | "DescribeReceiptRule"
        | "UpdateReceiptRule"
        | "CreateReceiptFilter"
        | "DeleteReceiptFilter"
        | "ListReceiptFilters" => Some("ses".to_string()),
        _ => None,
    }
}

/// Extract service name from the SigV4 Authorization header credential scope.
fn extract_service_from_auth(headers: &HeaderMap) -> Option<String> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let info = fakecloud_aws::sigv4::parse_sigv4(auth)?;
    Some(info.service)
}

/// Parse form-encoded body into key-value pairs.
pub fn parse_query_body(body: &Bytes) -> HashMap<String, String> {
    decode_form_urlencoded(body)
}

fn decode_form_urlencoded(input: &[u8]) -> HashMap<String, String> {
    let s = std::str::from_utf8(input).unwrap_or("");
    let mut result = HashMap::new();
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.find('=') {
            Some(pos) => (&pair[..pos], &pair[pos + 1..]),
            None => (pair, ""),
        };
        result.insert(url_decode(key), url_decode(value));
    }
    result
}

fn url_decode(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut bytes = input.bytes();
    while let Some(b) = bytes.next() {
        match b {
            b'+' => result.push(' '),
            b'%' => {
                let high = bytes.next().and_then(from_hex);
                let low = bytes.next().and_then(from_hex);
                if let (Some(h), Some(l)) = (high, low) {
                    result.push((h << 4 | l) as char);
                }
            }
            _ => result.push(b as char),
        }
    }
    result
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_amz_target_events() {
        let result = parse_amz_target("AWSEvents.PutEvents").unwrap();
        assert_eq!(result.service, "events");
        assert_eq!(result.action, "PutEvents");
        assert_eq!(result.protocol, AwsProtocol::Json);
    }

    #[test]
    fn parse_amz_target_ssm() {
        let result = parse_amz_target("AmazonSSM.GetParameter").unwrap();
        assert_eq!(result.service, "ssm");
        assert_eq!(result.action, "GetParameter");
    }

    #[test]
    fn parse_amz_target_kinesis() {
        let result = parse_amz_target("Kinesis_20131202.ListStreams").unwrap();
        assert_eq!(result.service, "kinesis");
        assert_eq!(result.action, "ListStreams");
        assert_eq!(result.protocol, AwsProtocol::Json);
    }

    #[test]
    fn parse_query_body_basic() {
        let body = Bytes::from(
            "Action=SendMessage&QueueUrl=http%3A%2F%2Flocalhost%3A4566%2Fqueue&MessageBody=hello",
        );
        let params = parse_query_body(&body);
        assert_eq!(params.get("Action").unwrap(), "SendMessage");
        assert_eq!(params.get("MessageBody").unwrap(), "hello");
    }
}
