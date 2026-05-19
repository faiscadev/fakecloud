//! `parse` concerns from rds/extras.rs (audit-2026-05-19).

use super::*;

pub(super) fn xml_empty_action(
    action: &str,
    request_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(xml_response_no_result(action, request_id))
}

/// Read a `<Name>.member.<N>` repeated query param into a Vec.
pub(super) fn parse_member_list(req: &AwsRequest, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    for i in 1.. {
        match get_param(req, &format!("{prefix}.member.{i}")) {
            Some(v) => out.push(v),
            None => break,
        }
    }
    out
}

/// Read repeated `Auth.member.<N>.{AuthScheme,SecretArn,IAMAuth,Description,ClientPasswordAuthType}`
/// proxy auth descriptors into a JSON array.
pub(super) fn parse_proxy_auth(req: &AwsRequest) -> Vec<Value> {
    let mut out = Vec::new();
    for i in 1.. {
        let scheme = get_param(req, &format!("Auth.member.{i}.AuthScheme"));
        let secret = get_param(req, &format!("Auth.member.{i}.SecretArn"));
        let iam = get_param(req, &format!("Auth.member.{i}.IAMAuth"));
        let desc = get_param(req, &format!("Auth.member.{i}.Description"));
        let pw = get_param(req, &format!("Auth.member.{i}.ClientPasswordAuthType"));
        if scheme.is_none() && secret.is_none() && iam.is_none() && desc.is_none() && pw.is_none() {
            break;
        }
        let mut entry = serde_json::Map::new();
        if let Some(v) = scheme {
            entry.insert("AuthScheme".to_string(), json!(v));
        }
        if let Some(v) = secret {
            entry.insert("SecretArn".to_string(), json!(v));
        }
        if let Some(v) = iam {
            entry.insert("IAMAuth".to_string(), json!(v));
        }
        if let Some(v) = desc {
            entry.insert("Description".to_string(), json!(v));
        }
        if let Some(v) = pw {
            entry.insert("ClientPasswordAuthType".to_string(), json!(v));
        }
        out.push(Value::Object(entry));
    }
    out
}

/// Read `OptionsToInclude.member.<N>.{OptionName,Port,OptionVersion}` plus
/// nested `DBSecurityGroupMemberships`/`VpcSecurityGroupMemberships` member
/// lists into a JSON array.
pub(super) fn parse_options_to_include(req: &AwsRequest) -> Vec<Value> {
    let mut out = Vec::new();
    for i in 1.. {
        let name = get_param(req, &format!("OptionsToInclude.member.{i}.OptionName"));
        let port = get_param(req, &format!("OptionsToInclude.member.{i}.Port"));
        let version = get_param(req, &format!("OptionsToInclude.member.{i}.OptionVersion"));
        if name.is_none() && port.is_none() && version.is_none() {
            break;
        }
        let mut entry = serde_json::Map::new();
        if let Some(v) = name {
            entry.insert("OptionName".to_string(), json!(v));
        }
        if let Some(v) = port {
            entry.insert("Port".to_string(), json!(v));
        }
        if let Some(v) = version {
            entry.insert("OptionVersion".to_string(), json!(v));
        }
        out.push(Value::Object(entry));
    }
    out
}

/// Pull resource type ("db", "cluster", "snapshot", ...) and id out of an
/// RDS ARN (`arn:aws:rds:region:account:type:id`). Bare ids fall back to
/// `("db", id)` so callers can pass instance identifiers directly.
pub(super) fn parse_rds_resource_arn(s: &str) -> (Option<&'static str>, String) {
    let parts: Vec<&str> = s.splitn(7, ':').collect();
    if parts.len() == 7 && parts[0] == "arn" && parts[2] == "rds" {
        let kind = match parts[5] {
            "db" => Some("db"),
            "cluster" => Some("cluster"),
            "snapshot" => Some("snapshot"),
            "cluster-snapshot" => Some("cluster-snapshot"),
            _ => None,
        };
        return (kind, parts[6].to_string());
    }
    (Some("db"), s.to_string())
}

/// Echo `KmsKeyId` as a full KMS ARN. Accepts a raw key id, an
/// `alias/<name>` reference, or an existing ARN (passed through).
pub(super) fn format_kms_arn(input: &str, region: &str, account_id: &str) -> String {
    if input.is_empty() {
        return String::new();
    }
    if input.starts_with("arn:") {
        return input.to_string();
    }
    if input.starts_with("alias/") {
        return Arn::new("kms", region, account_id, input).to_string();
    }
    Arn::new("kms", region, account_id, &format!("key/{input}")).to_string()
}

pub(super) fn list_extras_xml(
    svc: &RdsService,
    aid: &str,
    category: &str,
    wrapper: &str,
    action: &str,
    render: impl Fn(&Value) -> String,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state_handle().read();
    let items: Vec<Value> = accounts
        .get(aid)
        .and_then(|s| s.extras.get(category))
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default();
    let inner = format!(
        "    <{wrapper}>\n{}\n    </{wrapper}>",
        members(&items, render)
    );
    Ok(xml_response(action, inner, rid))
}
