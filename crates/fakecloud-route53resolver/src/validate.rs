//! Pure validation + construction helpers for Route 53 Resolver: error shapes,
//! id/ARN formatting, timestamp formatting, and input parsing. Kept free of any
//! shared-state references so both the awsJson handler and the CloudFormation
//! provisioner can reuse them.

use chrono::{SecondsFormat, Utc};
use http::StatusCode;
use serde_json::Value;
use uuid::Uuid;

use fakecloud_core::service::AwsServiceError;

use crate::state::{Tag, TargetAddress};

// ─── Error constructors (declared shape names from the model) ─────────────

pub fn invalid_parameter(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterException", msg)
}

pub fn invalid_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidRequestException", msg)
}

pub fn validation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

pub fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ResourceNotFoundException", msg)
}

pub fn resource_exists(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ResourceExistsException", msg)
}

pub fn resource_in_use(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ResourceInUseException", msg)
}

pub fn conflict(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ConflictException", msg)
}

pub fn unknown_resource(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "UnknownResourceException", msg)
}

pub fn invalid_next_token(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidNextTokenException", msg)
}

// ─── Ids / ARNs / timestamps ──────────────────────────────────────────────

/// A 17-hex-character resource suffix, matching the shape of real Route 53
/// Resolver ids (e.g. `rslvr-in-0123456789abcdef0`).
pub fn hex17() -> String {
    let u = Uuid::new_v4().simple().to_string();
    u[..17].to_string()
}

pub fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// `arn:aws:route53resolver:<region>:<account>:<kind>/<id>`.
pub fn arn(region: &str, account: &str, kind: &str, id: &str) -> String {
    format!("arn:aws:route53resolver:{region}:{account}:{kind}/{id}")
}

/// FNV-1a hash of a string, used to derive stable deterministic ids/suffixes.
pub fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 1469598103934665603;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h
}

/// Deterministically synthesize a VPC id for a subnet when EC2 state is not
/// wired (or the subnet is unknown), so `HostVPCId` is stable across calls for
/// the same subnet. Shared by the awsJson handler and the CloudFormation
/// provisioner so a CFN-provisioned endpoint carries a non-empty `HostVPCId`
/// just like the direct-API path.
pub fn synth_vpc(subnet_id: &str) -> String {
    format!("vpc-{:017x}", fnv1a(subnet_id) & 0x000f_ffff_ffff_ffff)
}

// ─── Input parsing ────────────────────────────────────────────────────────

/// Parse a Route 53 Resolver `TagList` (list of `{Key,Value}`).
pub fn parse_tags(v: Option<&Value>) -> Result<Vec<Tag>, AwsServiceError> {
    let Some(arr) = v.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for t in arr {
        let key = t
            .get("Key")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_parameter("Tag Key is required"))?
            .to_string();
        // Route 53 Resolver requires a (possibly empty) Value on every tag.
        let value = t
            .get("Value")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        out.push(Tag { key, value });
    }
    Ok(out)
}

/// Parse a `TargetList` (list of `TargetAddress`) for a FORWARD resolver rule.
pub fn parse_target_ips(v: Option<&Value>) -> Result<Vec<TargetAddress>, AwsServiceError> {
    let Some(arr) = v.and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for t in arr {
        let ip = t.get("Ip").and_then(Value::as_str).map(str::to_string);
        let ipv6 = t.get("Ipv6").and_then(Value::as_str).map(str::to_string);
        if ip.is_none() && ipv6.is_none() {
            return Err(invalid_parameter(
                "Each target must specify an Ip or Ipv6 address",
            ));
        }
        let port = t.get("Port").and_then(Value::as_i64);
        let protocol = t
            .get("Protocol")
            .and_then(Value::as_str)
            .map(str::to_string);
        let server_name_indication = t
            .get("ServerNameIndication")
            .and_then(Value::as_str)
            .map(str::to_string);
        out.push(TargetAddress {
            ip,
            // AWS defaults the DNS port to 53 when omitted.
            port: Some(port.unwrap_or(53)),
            ipv6,
            protocol,
            server_name_indication,
        });
    }
    Ok(out)
}

/// Extract a required string field or return `InvalidParameterException`.
pub fn required_str(body: &Value, field: &str) -> Result<String, AwsServiceError> {
    body.get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| invalid_parameter(format!("{field} is required")))
}
