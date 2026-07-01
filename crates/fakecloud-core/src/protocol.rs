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
const REST_XML_SERVICES: &[&str] = &["s3", "cloudfront", "route53"];

/// Services that use REST protocol with JSON responses (detected from SigV4 credential scope).
const REST_JSON_SERVICES: &[&str] = &[
    "lambda",
    "ses",
    "apigateway",
    "bedrock",
    "bedrock-agent",
    "bedrock-agent-runtime",
    "scheduler",
    "batch",
    "pipes",
    "rds-data",
    "dsql",
];

/// Detected service name and action from an incoming HTTP request.
#[derive(Debug, Clone)]
pub struct DetectedRequest {
    pub service: String,
    pub action: String,
    pub protocol: AwsProtocol,
}

/// Header-only service detection. Skips the form-encoded body sniff so
/// the dispatch path can decide whether to stream or buffer the body
/// without first reading it. Returns `None` when only a body sniff
/// would succeed; the caller must then fall back to [`detect_service`]
/// after buffering. Used to opt streaming routes (S3 PutObject /
/// UploadPart, ECR OCI v2 blob upload) out of the global body cap.
pub fn detect_service_headers_only(
    headers: &HeaderMap,
    query_params: &HashMap<String, String>,
) -> Option<DetectedRequest> {
    // Mirrors `detect_service` minus step 3 (form-body sniff).
    if let Some(target) = headers.get("x-amz-target").and_then(|v| v.to_str().ok()) {
        return parse_amz_target(target);
    }
    if let Some(action) = query_params.get("Action") {
        let service = extract_service_from_auth(headers)
            .or_else(|| infer_service_from_action(action))
            .or_else(|| parse_routing_host_from_headers(headers).map(|h| h.service));
        if let Some(service) = service {
            return Some(DetectedRequest {
                service,
                action: action.clone(),
                protocol: AwsProtocol::Query,
            });
        }
    }
    if let Some(service) = extract_service_from_auth(headers) {
        if let Some(protocol) = rest_protocol_for(&service) {
            return Some(DetectedRequest {
                service,
                action: String::new(),
                protocol,
            });
        }
    }
    if let Some(credential) = query_params.get("X-Amz-Credential") {
        let parts: Vec<&str> = credential.split('/').collect();
        if parts.len() >= 4 {
            let service = normalize_service_name(parts[3]).to_string();
            if let Some(protocol) = rest_protocol_for(&service) {
                return Some(DetectedRequest {
                    service,
                    action: String::new(),
                    protocol,
                });
            }
        }
    }
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
    if let Some(host_info) = parse_routing_host_from_headers(headers) {
        if let Some(protocol) = rest_protocol_for(&host_info.service) {
            return Some(DetectedRequest {
                service: host_info.service,
                action: String::new(),
                protocol,
            });
        }
    }
    None
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
        let service = extract_service_from_auth(headers)
            .or_else(|| infer_service_from_action(action))
            .or_else(|| parse_routing_host_from_headers(headers).map(|h| h.service));
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
            let service = extract_service_from_auth(headers)
                .or_else(|| infer_service_from_action(action))
                .or_else(|| parse_routing_host_from_headers(headers).map(|h| h.service));
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
            let service = normalize_service_name(parts[3]).to_string();
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

    // 7. Fallback: unsigned REST-style request carrying a LocalStack-shaped
    //    Host header. Lets fixtures and curl-style probes reach the right
    //    service without SigV4; signed requests were already handled in step 4.
    if let Some(host_info) = parse_routing_host_from_headers(headers) {
        if let Some(protocol) = rest_protocol_for(&host_info.service) {
            return Some(DetectedRequest {
                service: host_info.service,
                action: String::new(),
                protocol,
            });
        }
    }

    None
}

/// Service + region (and optional bucket) decoded from a `Host` header.
/// Covers both the LocalStack hostname convention
/// (`<service>.<region>.localhost.localstack.cloud[:port]`,
/// `<bucket>.s3.<region>.localhost.localstack.cloud[:port]`) and real AWS
/// service hostnames (`<service>.<region>.amazonaws.com`, S3 path-style
/// and virtual-hosted-style including the legacy no-region
/// `s3.amazonaws.com` / `<bucket>.s3.amazonaws.com` forms and the older
/// dash-separated `s3-<region>.amazonaws.com` form).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingHost {
    pub service: String,
    pub region: String,
    /// Set only for virtual-hosted-style S3 hostnames.
    pub bucket: Option<String>,
}

const LOCALSTACK_SUFFIX: &str = ".localhost.localstack.cloud";
const AWS_SUFFIX: &str = ".amazonaws.com";

/// Parse a `Host` header value for a LocalStack- or AWS-shaped hostname.
/// Returns `None` for anything that doesn't match — callers fall through
/// to their existing detection path.
pub fn parse_routing_host(host: &str) -> Option<RoutingHost> {
    let hostname = host.split(':').next()?;
    if hostname.is_empty() {
        return None;
    }
    let hostname = hostname.to_ascii_lowercase();
    if let Some(prefix) = hostname.strip_suffix(LOCALSTACK_SUFFIX) {
        return parse_localstack_prefix(prefix);
    }
    if hostname == "amazonaws.com" {
        return None;
    }
    if let Some(prefix) = hostname.strip_suffix(AWS_SUFFIX) {
        return parse_aws_prefix(prefix);
    }
    None
}

/// Pull the `Host` header and parse it with [`parse_routing_host`].
pub fn parse_routing_host_from_headers(headers: &HeaderMap) -> Option<RoutingHost> {
    let host = headers.get("host")?.to_str().ok()?;
    parse_routing_host(host)
}

fn parse_localstack_prefix(prefix: &str) -> Option<RoutingHost> {
    if prefix.is_empty() {
        return None;
    }
    let labels: Vec<&str> = prefix.split('.').collect();
    if labels.iter().any(|l| l.is_empty()) {
        return None;
    }
    match labels.len() {
        2 => Some(RoutingHost {
            service: labels[0].to_string(),
            region: labels[1].to_string(),
            bucket: None,
        }),
        n if n >= 3 && labels[n - 2] == "s3" => {
            let bucket = labels[..n - 2].join(".");
            Some(RoutingHost {
                service: "s3".to_string(),
                region: labels[n - 1].to_string(),
                bucket: Some(bucket),
            })
        }
        n if n >= 3 && labels[n - 2] == "s3-accesspoint" => {
            let bucket = labels[..n - 2].join(".");
            Some(RoutingHost {
                service: "s3".to_string(),
                region: labels[n - 1].to_string(),
                bucket: Some(bucket),
            })
        }
        n if n >= 3 && labels[n - 2] == "s3-control" => Some(RoutingHost {
            service: "s3".to_string(),
            region: labels[n - 1].to_string(),
            bucket: None,
        }),
        _ => None,
    }
}

/// Parse the prefix before `.amazonaws.com`.
///
/// Handles every variant AWS has shipped for the common REST/Query services:
///
/// - `<service>.<region>` — modern regional endpoint (most services).
/// - `s3.<region>` — modern path-style S3.
/// - `<bucket>.s3.<region>` — modern virtual-hosted S3 (bucket may contain dots).
/// - `s3` — legacy S3 global endpoint (implicitly `us-east-1`).
/// - `<bucket>.s3` — legacy virtual-hosted S3 (implicitly `us-east-1`).
/// - `s3-<region>` — older dash-separated path-style S3.
/// - `<bucket>.s3-<region>` — older dash-separated virtual-hosted S3.
fn parse_aws_prefix(prefix: &str) -> Option<RoutingHost> {
    if prefix.is_empty() {
        return None;
    }
    let labels: Vec<&str> = prefix.split('.').collect();
    if labels.iter().any(|l| l.is_empty()) {
        return None;
    }
    let last = *labels.last()?;

    // `s3-<region>` as the last label: dash-separated S3. Bucket, if any,
    // is whatever precedes it.
    if let Some(region) = last.strip_prefix("s3-") {
        if !region.is_empty() {
            let bucket = if labels.len() >= 2 {
                Some(labels[..labels.len() - 1].join("."))
            } else {
                None
            };
            return Some(RoutingHost {
                service: "s3".to_string(),
                region: region.to_string(),
                bucket,
            });
        }
    }

    // Legacy global S3: last label is `s3`, no region present. `s3` on its
    // own is the path-style global endpoint; anything preceding it is the
    // bucket (including dotted names like `a.b.s3.amazonaws.com`).
    if last == "s3" {
        if labels.len() == 1 {
            return Some(RoutingHost {
                service: "s3".to_string(),
                region: "us-east-1".to_string(),
                bucket: None,
            });
        }
        return Some(RoutingHost {
            service: "s3".to_string(),
            region: "us-east-1".to_string(),
            bucket: Some(labels[..labels.len() - 1].join(".")),
        });
    }

    // `s3-accesspoint.<region>` — path-style access point endpoint.
    // `{alias}-{account-id}.s3-accesspoint.<region>` — virtual-hosted access point.
    if last == "s3-accesspoint" {
        if labels.len() == 2 {
            return Some(RoutingHost {
                service: "s3".to_string(),
                region: labels[0].to_string(),
                bucket: None,
            });
        }
        // Virtual-hosted form needs at least {alias}.{region}.s3-accesspoint, i.e.
        // 3+ labels. A bare "s3-accesspoint" host (1 label) must not reach the
        // `len() - 2` slice, which would underflow and panic.
        if labels.len() >= 3 {
            let bucket = labels[..labels.len() - 2].join(".");
            return Some(RoutingHost {
                service: "s3".to_string(),
                region: labels[labels.len() - 1].to_string(),
                bucket: Some(bucket),
            });
        }
    }

    // `s3-control.<region>` or `{account-id}.s3-control.<region>` — S3
    // Control endpoint (access point management).
    if labels.len() >= 2 && labels[labels.len() - 2] == "s3-control" {
        return Some(RoutingHost {
            service: "s3".to_string(),
            region: last.to_string(),
            bucket: None,
        });
    }

    match labels.len() {
        // `<service>.<region>` — the common case. Covers `s3.<region>`
        // path-style S3 too, since the service label falls through here.
        2 => Some(RoutingHost {
            service: labels[0].to_string(),
            region: labels[1].to_string(),
            bucket: None,
        }),
        // `<bucket>.s3.<region>` — modern virtual-hosted S3.
        n if n >= 3 && labels[n - 2] == "s3" => {
            let bucket = labels[..n - 2].join(".");
            Some(RoutingHost {
                service: "s3".to_string(),
                region: labels[n - 1].to_string(),
                bucket: Some(bucket),
            })
        }
        _ => None,
    }
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
        "DynamoDBStreams_20120810" => "dynamodbstreams",
        "Logs_20140328" => "logs",
        s if s.starts_with("secretsmanager") => "secretsmanager",
        s if s.starts_with("TrentService") => "kms",
        s if s.starts_with("AWSCognitoIdentityProviderService") => "cognito-idp",
        s if s.starts_with("AWSCognitoIdentityService") => "cognito-identity",
        s if s.starts_with("Kinesis_20131202") => "kinesis",
        s if s.starts_with("AmazonEC2ContainerRegistry_V") => "ecr",
        s if s.starts_with("AmazonEC2ContainerServiceV") => "ecs",
        s if s.starts_with("AWSStepFunctions") => "states",
        s if s.starts_with("AWSOrganizationsV") => "organizations",
        "CertificateManager" => "acm",
        "AnyScaleFrontendService" => "application-autoscaling",
        // Match the WAFv2 target version exactly so legacy WAF Classic
        // (`AWSWAF_*` without the `_20190729` suffix) doesn't get routed here.
        "AWSWAF_20190729" => "wafv2",
        "AmazonAthena" => "athena",
        s if s.starts_with("Firehose_") => "firehose",
        "AWSGlue" => "glue",
        "CloudApiService" => "cloudcontrolapi",
        // CloudWatch advertises awsJson1_0 (target service shape
        // `GraniteServiceVersion20100801`) alongside the legacy awsQuery
        // protocol. Newer SDKs (aws-sdk-rust / js-v3 / go-v2) POST with
        // `X-Amz-Target: GraniteServiceVersion20100801.<Operation>` and a JSON
        // body. The service registry key is `monitoring`.
        s if s.starts_with("GraniteServiceVersion") => "monitoring",
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
        // SNS subscription handshake: the SubscribeURL / UnsubscribeUrl that SNS
        // hands to HTTP/S and email subscribers are unsigned bare GETs (no auth
        // header), so the service must be inferred from the action alone.
        "ConfirmSubscription" | "Unsubscribe" => Some("sns".to_string()),
        _ => None,
    }
}

/// Extract service name from the SigV4 Authorization header credential scope.
fn extract_service_from_auth(headers: &HeaderMap) -> Option<String> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let info = fakecloud_aws::sigv4::parse_sigv4(auth)?;
    Some(normalize_service_name(&info.service).to_string())
}

/// Map AWS service-name aliases that share path namespace and handlers
/// to the canonical form used by fakecloud's service registry.
///
/// AWS uses `bedrock-runtime` in the SigV4 credential scope of runtime
/// API calls (`InvokeModel`, `ApplyGuardrail`, etc.) but the REST paths
/// (e.g. `POST /guardrail/{id}/version/{ver}/apply`) live under the same
/// `BedrockService` handler that owns the control-plane `bedrock` paths.
/// Without normalization, `detect_service` returns `None` for
/// `bedrock-runtime` (not in `REST_JSON_SERVICES`), the central
/// dispatcher falls back to API Gateway, and `/guardrail/...` 404s with
/// `NotFoundException: Stage not found: guardrail`. See issue #1232.
fn normalize_service_name(service: &str) -> &str {
    match service {
        "bedrock-runtime" => "bedrock",
        // Real AWS API Gateway V2 SDK signs with `apigateway` as the SigV4
        // service (per the model's `aws.api#service.endpointPrefix`), but
        // tools driven by the Smithy service shape name (including our own
        // conformance probe) may send `apigatewayv2`. Both refer to the
        // same fakecloud service registry entry — the v2 handler is path-
        // routed under `/v2/...` and the v1 handler under `/restapis/...`,
        // both reachable behind the `apigateway` SigV4 service.
        "apigatewayv2" => "apigateway",
        other => other,
    }
}

/// Parse form-encoded body into key-value pairs.
pub fn parse_query_body(body: &Bytes) -> HashMap<String, String> {
    decode_form_urlencoded(body)
}

/// Flatten an awsJson request body into the flat `awsQuery` key form that
/// query-protocol handlers consume.
///
/// CloudWatch is served by handlers written against the awsQuery flat-key map
/// (`MetricData.member.1.MetricName`, `StatisticValues.Sum`,
/// `Dimensions.member.2.Value`, ...). Its Smithy model also advertises
/// `awsJson1_0`, so modern SDKs send a nested JSON body instead. Rather than
/// duplicate every parser, we flatten the JSON into the same map the awsQuery
/// handlers already read:
///
/// - object field `K` -> key `K` (or `<parent>.K` when nested in a struct)
/// - array element `i` (1-based) -> `<K>.member.<i>` (matching the awsQuery
///   list wire convention)
/// - scalars -> their string form (numbers/booleans stringified)
///
/// A body that is not a JSON object yields an empty map.
pub fn flatten_json_to_query(body: &Bytes) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return out;
    };
    if value.is_object() {
        flatten_json_value("", &value, &mut out);
    }
    out
}

fn flatten_json_value(prefix: &str, value: &serde_json::Value, out: &mut HashMap<String, String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let child = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten_json_value(&child, v, out);
            }
        }
        serde_json::Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                let child = format!("{prefix}.member.{}", i + 1);
                flatten_json_value(&child, v, out);
            }
        }
        serde_json::Value::Null => {}
        serde_json::Value::String(s) => {
            out.insert(prefix.to_string(), s.clone());
        }
        serde_json::Value::Bool(b) => {
            out.insert(prefix.to_string(), b.to_string());
        }
        serde_json::Value::Number(n) => {
            out.insert(prefix.to_string(), n.to_string());
        }
    }
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

    #[test]
    fn parse_query_body_empty_returns_empty_map() {
        let body = Bytes::from("");
        let params = parse_query_body(&body);
        assert!(params.is_empty());
    }

    #[test]
    fn parse_query_body_duplicate_keys_last_wins() {
        let body = Bytes::from("key=a&key=b");
        let params = parse_query_body(&body);
        assert_eq!(params.get("key").unwrap(), "b");
    }

    #[test]
    fn parse_query_body_single_key() {
        let body = Bytes::from("key=value");
        let params = parse_query_body(&body);
        assert_eq!(params.get("key").unwrap(), "value");
    }

    #[test]
    fn parse_amz_target_ecs() {
        let result = parse_amz_target("AmazonEC2ContainerServiceV20141113.ListClusters").unwrap();
        assert_eq!(result.service, "ecs");
        assert_eq!(result.action, "ListClusters");
        assert_eq!(result.protocol, AwsProtocol::Json);
    }

    #[test]
    fn parse_amz_target_invalid_returns_none() {
        assert!(parse_amz_target("NoDotHere").is_none());
        assert!(parse_amz_target("").is_none());
    }

    #[test]
    fn parse_amz_target_cloudwatch_json() {
        // CloudWatch's awsJson1_0 target service shape.
        let result = parse_amz_target("GraniteServiceVersion20100801.PutMetricData").unwrap();
        assert_eq!(result.service, "monitoring");
        assert_eq!(result.action, "PutMetricData");
        assert_eq!(result.protocol, AwsProtocol::Json);
    }

    #[test]
    fn flatten_json_to_query_nested() {
        let body = Bytes::from(
            serde_json::json!({
                "Namespace": "MyApp",
                "MetricData": [{
                    "MetricName": "Latency",
                    "Value": 12.5,
                    "StatisticValues": {"SampleCount": 3, "Sum": 10},
                    "Dimensions": [{"Name": "Endpoint", "Value": "/api"}]
                }]
            })
            .to_string(),
        );
        let flat = flatten_json_to_query(&body);
        assert_eq!(flat.get("Namespace").unwrap(), "MyApp");
        assert_eq!(
            flat.get("MetricData.member.1.MetricName").unwrap(),
            "Latency"
        );
        assert_eq!(flat.get("MetricData.member.1.Value").unwrap(), "12.5");
        assert_eq!(
            flat.get("MetricData.member.1.StatisticValues.SampleCount")
                .unwrap(),
            "3"
        );
        assert_eq!(
            flat.get("MetricData.member.1.Dimensions.member.1.Name")
                .unwrap(),
            "Endpoint"
        );
        assert_eq!(
            flat.get("MetricData.member.1.Dimensions.member.1.Value")
                .unwrap(),
            "/api"
        );
    }

    #[test]
    fn flatten_json_to_query_non_object_is_empty() {
        assert!(flatten_json_to_query(&Bytes::from_static(b"[]")).is_empty());
        assert!(flatten_json_to_query(&Bytes::from_static(b"not json")).is_empty());
    }

    #[test]
    fn parse_amz_target_various_prefixes() {
        assert_eq!(
            parse_amz_target("AmazonSQS.SendMessage").unwrap().service,
            "sqs"
        );
        assert_eq!(
            parse_amz_target("AmazonSNS.Publish").unwrap().service,
            "sns"
        );
        assert_eq!(
            parse_amz_target("DynamoDB_20120810.GetItem")
                .unwrap()
                .service,
            "dynamodb"
        );
        assert_eq!(
            parse_amz_target("Logs_20140328.PutLogEvents")
                .unwrap()
                .service,
            "logs"
        );
        assert_eq!(
            parse_amz_target("secretsmanager.GetSecretValue")
                .unwrap()
                .service,
            "secretsmanager"
        );
        assert_eq!(
            parse_amz_target("TrentService.Encrypt").unwrap().service,
            "kms"
        );
        assert_eq!(
            parse_amz_target("AWSCognitoIdentityProviderService.InitiateAuth")
                .unwrap()
                .service,
            "cognito-idp"
        );
        assert_eq!(
            parse_amz_target("AWSStepFunctions.StartExecution")
                .unwrap()
                .service,
            "states"
        );
        assert_eq!(
            parse_amz_target("AWSOrganizationsV20161128.CreateOrganization")
                .unwrap()
                .service,
            "organizations"
        );
        assert!(parse_amz_target("UnknownServicePrefix.Action").is_none());
    }

    #[test]
    fn infer_service_from_action_maps_sts() {
        assert_eq!(
            infer_service_from_action("AssumeRole").as_deref(),
            Some("sts")
        );
        assert_eq!(
            infer_service_from_action("GetCallerIdentity").as_deref(),
            Some("sts")
        );
    }

    #[test]
    fn infer_service_from_action_maps_iam() {
        assert_eq!(
            infer_service_from_action("CreateUser").as_deref(),
            Some("iam")
        );
        assert_eq!(
            infer_service_from_action("ListRoles").as_deref(),
            Some("iam")
        );
    }

    #[test]
    fn infer_service_from_action_maps_ses() {
        assert_eq!(
            infer_service_from_action("SendEmail").as_deref(),
            Some("ses")
        );
        assert_eq!(
            infer_service_from_action("ListIdentities").as_deref(),
            Some("ses")
        );
    }

    #[test]
    fn infer_service_from_action_maps_sns_confirmation_flow() {
        // SNS hands subscribers unsigned SubscribeURL / UnsubscribeUrl GETs,
        // so the service must be inferred from the action alone.
        assert_eq!(
            infer_service_from_action("ConfirmSubscription").as_deref(),
            Some("sns")
        );
        assert_eq!(
            infer_service_from_action("Unsubscribe").as_deref(),
            Some("sns")
        );
    }

    #[test]
    fn detect_service_routes_unsigned_confirm_subscription_to_sns() {
        // Mirror the bare GET an HTTP/S subscriber issues at the SubscribeURL:
        // no Authorization header, bare-localhost Host, Action in the query.
        let mut headers = HeaderMap::new();
        headers.insert("host", "localhost:4566".parse().unwrap());
        let mut query_params = HashMap::new();
        query_params.insert("Action".to_string(), "ConfirmSubscription".to_string());
        query_params.insert(
            "TopicArn".to_string(),
            "arn:aws:sns:us-east-1:000000000000:t".to_string(),
        );
        query_params.insert("Token".to_string(), "abc123".to_string());

        let detected = detect_service(&headers, &query_params, &Bytes::new())
            .expect("ConfirmSubscription must route to a service");
        assert_eq!(detected.service, "sns");
        assert_eq!(detected.action, "ConfirmSubscription");
        assert_eq!(detected.protocol, AwsProtocol::Query);
    }

    #[test]
    fn infer_service_from_action_unknown_returns_none() {
        assert!(infer_service_from_action("NotARealAction").is_none());
    }

    #[test]
    fn rest_protocol_for_returns_none_for_non_rest_service() {
        assert!(rest_protocol_for("sqs").is_none());
    }

    #[test]
    fn url_decode_handles_percent_and_plus() {
        assert_eq!(url_decode("hello+world"), "hello world");
        assert_eq!(url_decode("hello%20world"), "hello world");
        assert_eq!(url_decode("100%25"), "100%");
    }

    #[test]
    fn url_decode_ignores_malformed_percent() {
        assert_eq!(url_decode("%ZZ"), "");
    }

    #[test]
    fn from_hex_valid_digits() {
        assert_eq!(from_hex(b'0'), Some(0));
        assert_eq!(from_hex(b'9'), Some(9));
        assert_eq!(from_hex(b'a'), Some(10));
        assert_eq!(from_hex(b'F'), Some(15));
    }

    #[test]
    fn from_hex_invalid_returns_none() {
        assert!(from_hex(b'g').is_none());
        assert!(from_hex(b' ').is_none());
    }

    #[test]
    fn detect_service_via_amz_target() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-target", "AmazonSSM.GetParameter".parse().unwrap());
        let query = HashMap::new();
        let body = Bytes::new();
        let detected = detect_service(&headers, &query, &body).unwrap();
        assert_eq!(detected.service, "ssm");
        assert_eq!(detected.action, "GetParameter");
    }

    #[test]
    fn detect_service_via_query_action_with_inferred_service() {
        let headers = HeaderMap::new();
        let mut query = HashMap::new();
        query.insert("Action".to_string(), "AssumeRole".to_string());
        let body = Bytes::new();
        let detected = detect_service(&headers, &query, &body).unwrap();
        assert_eq!(detected.service, "sts");
        assert_eq!(detected.action, "AssumeRole");
        assert_eq!(detected.protocol, AwsProtocol::Query);
    }

    #[test]
    fn detect_service_via_form_body() {
        let headers = HeaderMap::new();
        let query = HashMap::new();
        let body = Bytes::from("Action=SendEmail&Source=x%40y.com");
        let detected = detect_service(&headers, &query, &body).unwrap();
        assert_eq!(detected.service, "ses");
        assert_eq!(detected.action, "SendEmail");
    }

    #[test]
    fn detect_service_via_sigv2_presigned() {
        let headers = HeaderMap::new();
        let mut query = HashMap::new();
        query.insert("AWSAccessKeyId".to_string(), "AKID".to_string());
        query.insert("Signature".to_string(), "sig".to_string());
        query.insert("Expires".to_string(), "1234567890".to_string());
        let body = Bytes::new();
        let detected = detect_service(&headers, &query, &body).unwrap();
        assert_eq!(detected.service, "s3");
        assert_eq!(detected.protocol, AwsProtocol::Rest);
    }

    #[test]
    fn detect_service_via_sigv4_presigned_credential() {
        let headers = HeaderMap::new();
        let mut query = HashMap::new();
        query.insert(
            "X-Amz-Credential".to_string(),
            "AKID/20240101/us-east-1/s3/aws4_request".to_string(),
        );
        let body = Bytes::new();
        let detected = detect_service(&headers, &query, &body).unwrap();
        assert_eq!(detected.service, "s3");
        assert_eq!(detected.protocol, AwsProtocol::Rest);
    }

    #[test]
    fn detect_service_unknown_returns_none() {
        let headers = HeaderMap::new();
        let query = HashMap::new();
        let body = Bytes::new();
        assert!(detect_service(&headers, &query, &body).is_none());
    }

    #[test]
    fn normalize_service_name_aliases_apigatewayv2_to_apigateway() {
        // Real AWS API Gateway V2 SDK signs with `apigateway` per the
        // model's `endpointPrefix`, but Smithy-driven tooling (including
        // our conformance probe) sends `apigatewayv2`. Both routes resolve
        // to the same fakecloud service registry entry.
        assert_eq!(normalize_service_name("apigatewayv2"), "apigateway");
    }

    #[test]
    fn normalize_service_name_aliases_bedrock_runtime_to_bedrock() {
        // The bedrock-runtime credential scope shares path namespace with
        // the bedrock control plane (`POST /guardrail/{id}/version/{ver}/apply`
        // is implemented under BedrockService). Routing must resolve to
        // the bedrock service so the existing handlers run. See #1232.
        assert_eq!(normalize_service_name("bedrock-runtime"), "bedrock");
    }

    #[test]
    fn normalize_service_name_passes_through_unaliased_services() {
        // Every service that isn't on the alias list must round-trip
        // unchanged — including the canonical bedrock name itself, so a
        // plain bedrock request takes the same code path it always has.
        assert_eq!(normalize_service_name("bedrock"), "bedrock");
        assert_eq!(normalize_service_name("s3"), "s3");
        assert_eq!(normalize_service_name("lambda"), "lambda");
        assert_eq!(normalize_service_name(""), "");
        assert_eq!(
            normalize_service_name("unknown-future-service"),
            "unknown-future-service"
        );
    }

    #[test]
    fn detect_service_via_authorization_header_normalizes_bedrock_runtime() {
        // SigV4 auth header carries `bedrock-runtime` in the credential
        // scope; dispatcher must route to the bedrock service handler so
        // `/guardrail/...` lands on `BedrockService` instead of falling
        // through to API Gateway.
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            "AWS4-HMAC-SHA256 \
             Credential=AKID/20240101/us-east-1/bedrock-runtime/aws4_request, \
             SignedHeaders=host, Signature=abc"
                .parse()
                .unwrap(),
        );
        let query = HashMap::new();
        let body = Bytes::new();
        let detected = detect_service(&headers, &query, &body).unwrap();
        assert_eq!(detected.service, "bedrock");
        assert_eq!(detected.protocol, AwsProtocol::RestJson);
    }

    #[test]
    fn detect_service_via_sigv4_presigned_credential_normalizes_bedrock_runtime() {
        // Same alias normalization on the presigned-URL path: a request
        // signed with bedrock-runtime in the X-Amz-Credential query param
        // must still resolve to the bedrock service handler.
        let headers = HeaderMap::new();
        let mut query = HashMap::new();
        query.insert(
            "X-Amz-Credential".to_string(),
            "AKID/20240101/us-east-1/bedrock-runtime/aws4_request".to_string(),
        );
        let body = Bytes::new();
        let detected = detect_service(&headers, &query, &body).unwrap();
        assert_eq!(detected.service, "bedrock");
        assert_eq!(detected.protocol, AwsProtocol::RestJson);
    }

    #[test]
    fn parse_routing_host_localstack_basic() {
        let h = parse_routing_host("sqs.us-east-1.localhost.localstack.cloud").unwrap();
        assert_eq!(h.service, "sqs");
        assert_eq!(h.region, "us-east-1");
        assert!(h.bucket.is_none());
    }

    #[test]
    fn parse_routing_host_localstack_with_port() {
        let h = parse_routing_host("lambda.eu-west-1.localhost.localstack.cloud:4566").unwrap();
        assert_eq!(h.service, "lambda");
        assert_eq!(h.region, "eu-west-1");
        assert!(h.bucket.is_none());
    }

    #[test]
    fn parse_routing_host_case_insensitive() {
        let h = parse_routing_host("SQS.US-EAST-1.LOCALHOST.LOCALSTACK.CLOUD:4566").unwrap();
        assert_eq!(h.service, "sqs");
        assert_eq!(h.region, "us-east-1");

        let h = parse_routing_host("LAMBDA.US-EAST-1.AMAZONAWS.COM").unwrap();
        assert_eq!(h.service, "lambda");
        assert_eq!(h.region, "us-east-1");
    }

    #[test]
    fn parse_routing_host_localstack_s3_virtual_hosted() {
        let h =
            parse_routing_host("my-bucket.s3.us-east-1.localhost.localstack.cloud:4566").unwrap();
        assert_eq!(h.service, "s3");
        assert_eq!(h.region, "us-east-1");
        assert_eq!(h.bucket.as_deref(), Some("my-bucket"));
    }

    #[test]
    fn parse_routing_host_localstack_s3_vhost_bucket_with_dots() {
        let h = parse_routing_host("a.b.c.s3.us-east-1.localhost.localstack.cloud").unwrap();
        assert_eq!(h.service, "s3");
        assert_eq!(h.region, "us-east-1");
        assert_eq!(h.bucket.as_deref(), Some("a.b.c"));
    }

    #[test]
    fn parse_routing_host_aws_service_region() {
        let h = parse_routing_host("sqs.us-east-1.amazonaws.com").unwrap();
        assert_eq!(h.service, "sqs");
        assert_eq!(h.region, "us-east-1");
        assert!(h.bucket.is_none());

        let h = parse_routing_host("dynamodb.eu-west-2.amazonaws.com:443").unwrap();
        assert_eq!(h.service, "dynamodb");
        assert_eq!(h.region, "eu-west-2");
    }

    #[test]
    fn parse_routing_host_aws_s3_path_style_modern() {
        let h = parse_routing_host("s3.us-east-1.amazonaws.com").unwrap();
        assert_eq!(h.service, "s3");
        assert_eq!(h.region, "us-east-1");
        assert!(h.bucket.is_none());
    }

    #[test]
    fn parse_routing_host_aws_s3_virtual_hosted_modern() {
        let h = parse_routing_host("my-bucket.s3.us-east-1.amazonaws.com").unwrap();
        assert_eq!(h.service, "s3");
        assert_eq!(h.region, "us-east-1");
        assert_eq!(h.bucket.as_deref(), Some("my-bucket"));
    }

    #[test]
    fn parse_routing_host_aws_s3_vhost_bucket_with_dots() {
        let h = parse_routing_host("a.b.c.s3.us-east-1.amazonaws.com").unwrap();
        assert_eq!(h.service, "s3");
        assert_eq!(h.region, "us-east-1");
        assert_eq!(h.bucket.as_deref(), Some("a.b.c"));
    }

    #[test]
    fn parse_routing_host_aws_s3_legacy_global() {
        // `s3.amazonaws.com` (no region) is the legacy S3 global endpoint —
        // AWS treats it as us-east-1 for both path-style and virtual-hosted.
        let h = parse_routing_host("s3.amazonaws.com").unwrap();
        assert_eq!(h.service, "s3");
        assert_eq!(h.region, "us-east-1");
        assert!(h.bucket.is_none());

        let h = parse_routing_host("my-bucket.s3.amazonaws.com").unwrap();
        assert_eq!(h.service, "s3");
        assert_eq!(h.region, "us-east-1");
        assert_eq!(h.bucket.as_deref(), Some("my-bucket"));
    }

    #[test]
    fn parse_routing_host_aws_s3_legacy_global_dotted_bucket() {
        // AWS allows buckets with dots (e.g. `a.b.c`) and still serves them
        // via the legacy `<bucket>.s3.amazonaws.com` global endpoint.
        let h = parse_routing_host("a.b.c.s3.amazonaws.com").unwrap();
        assert_eq!(h.service, "s3");
        assert_eq!(h.region, "us-east-1");
        assert_eq!(h.bucket.as_deref(), Some("a.b.c"));
    }

    #[test]
    fn parse_routing_host_aws_s3_dash_separated() {
        // Older dash-separated form still served by AWS.
        let h = parse_routing_host("s3-us-west-2.amazonaws.com").unwrap();
        assert_eq!(h.service, "s3");
        assert_eq!(h.region, "us-west-2");
        assert!(h.bucket.is_none());

        let h = parse_routing_host("my-bucket.s3-us-west-2.amazonaws.com").unwrap();
        assert_eq!(h.service, "s3");
        assert_eq!(h.region, "us-west-2");
        assert_eq!(h.bucket.as_deref(), Some("my-bucket"));
    }

    #[test]
    fn parse_routing_host_rejects_plain_localhost() {
        assert!(parse_routing_host("localhost:4566").is_none());
        assert!(parse_routing_host("127.0.0.1:4566").is_none());
    }

    #[test]
    fn parse_routing_host_rejects_unknown_suffix() {
        assert!(parse_routing_host("sqs.us-east-1.example.com").is_none());
        assert!(parse_routing_host("s3.us-east-1.aws").is_none());
    }

    #[test]
    fn parse_routing_host_empty_and_malformed_rejected() {
        assert!(parse_routing_host("").is_none());
        assert!(parse_routing_host(".localhost.localstack.cloud").is_none());
        assert!(parse_routing_host("..localhost.localstack.cloud").is_none());
        assert!(parse_routing_host("sqs.localhost.localstack.cloud").is_none());
        assert!(parse_routing_host("foo.bar.baz.localhost.localstack.cloud").is_none());
        assert!(parse_routing_host(".amazonaws.com").is_none());
        assert!(parse_routing_host("amazonaws.com").is_none());
    }

    #[test]
    fn parse_routing_host_bare_s3_accesspoint_does_not_panic() {
        // A single-label "s3-accesspoint" host has < 2 labels, so the
        // virtual-hosted `len() - 2` slice would underflow and panic without
        // the length guard. It must be rejected, not crash the router.
        assert!(parse_routing_host("s3-accesspoint").is_none());
    }

    #[test]
    fn detect_service_via_host_for_rest_service() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "host",
            "s3.us-east-1.localhost.localstack.cloud:4566"
                .parse()
                .unwrap(),
        );
        let query = HashMap::new();
        let body = Bytes::new();
        let detected = detect_service(&headers, &query, &body).unwrap();
        assert_eq!(detected.service, "s3");
        assert_eq!(detected.protocol, AwsProtocol::Rest);
    }

    #[test]
    fn detect_service_via_host_for_rest_json_service() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "host",
            "lambda.us-east-1.localhost.localstack.cloud:4566"
                .parse()
                .unwrap(),
        );
        let query = HashMap::new();
        let body = Bytes::new();
        let detected = detect_service(&headers, &query, &body).unwrap();
        assert_eq!(detected.service, "lambda");
        assert_eq!(detected.protocol, AwsProtocol::RestJson);
    }

    #[test]
    fn detect_service_via_host_plus_query_action() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "host",
            "sqs.us-east-1.localhost.localstack.cloud:4566"
                .parse()
                .unwrap(),
        );
        let mut query = HashMap::new();
        query.insert("Action".to_string(), "ListQueues".to_string());
        let body = Bytes::new();
        let detected = detect_service(&headers, &query, &body).unwrap();
        assert_eq!(detected.service, "sqs");
        assert_eq!(detected.action, "ListQueues");
        assert_eq!(detected.protocol, AwsProtocol::Query);
    }

    #[test]
    fn detect_service_sigv4_wins_over_host() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=abc"
                .parse()
                .unwrap(),
        );
        headers.insert(
            "host",
            "lambda.us-east-1.localhost.localstack.cloud:4566"
                .parse()
                .unwrap(),
        );
        let query = HashMap::new();
        let body = Bytes::new();
        let detected = detect_service(&headers, &query, &body).unwrap();
        // SigV4 credential scope says s3; Host header says lambda. SigV4 wins.
        assert_eq!(detected.service, "s3");
        assert_eq!(detected.protocol, AwsProtocol::Rest);
    }

    #[test]
    fn detect_service_host_for_virtual_hosted_s3() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "host",
            "my-bucket.s3.us-east-1.localhost.localstack.cloud:4566"
                .parse()
                .unwrap(),
        );
        let query = HashMap::new();
        let body = Bytes::new();
        let detected = detect_service(&headers, &query, &body).unwrap();
        assert_eq!(detected.service, "s3");
        assert_eq!(detected.protocol, AwsProtocol::Rest);
    }
}
