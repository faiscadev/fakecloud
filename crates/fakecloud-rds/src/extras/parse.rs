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
        // Capture nested security-group memberships — the doc-comment
        // promised them but the loop above never read them, so
        // ModifyOptionGroup was dropping that part of the request.
        let mut dbsg = Vec::new();
        for j in 1.. {
            let id = get_param(
                req,
                &format!("OptionsToInclude.member.{i}.DBSecurityGroupMemberships.member.{j}"),
            );
            match id {
                Some(v) => dbsg.push(json!(v)),
                None => break,
            }
        }
        if !dbsg.is_empty() {
            entry.insert("DBSecurityGroupMemberships".to_string(), Value::Array(dbsg));
        }
        let mut vpcsg = Vec::new();
        for j in 1.. {
            let id = get_param(
                req,
                &format!("OptionsToInclude.member.{i}.VpcSecurityGroupMemberships.member.{j}"),
            );
            match id {
                Some(v) => vpcsg.push(json!(v)),
                None => break,
            }
        }
        if !vpcsg.is_empty() {
            entry.insert(
                "VpcSecurityGroupMemberships".to_string(),
                Value::Array(vpcsg),
            );
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

/// Render a whole extras category as a Describe list response.
///
/// `member_tag` is the element each entry is wrapped in: the list's
/// Smithy `xmlName` where the model declares one (the AWS SDKs unmarshal
/// an empty list from the generic `<member>`, so getting this wrong makes
/// the rows invisible to every real client), or `"member"` for the few
/// lists that genuinely have no member xmlName.
#[allow(clippy::too_many_arguments)]
pub(super) fn list_extras_xml(
    svc: &RdsService,
    aid: &str,
    category: &str,
    wrapper: &str,
    member_tag: &str,
    action: &str,
    render: impl Fn(&Value) -> String,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    list_extras_filtered_xml(
        svc,
        aid,
        category,
        wrapper,
        member_tag,
        action,
        |_| true,
        render,
        rid,
    )
}

/// [`list_extras_xml`] with a predicate, for the Describe operations that
/// model `Filters`.
///
/// Rows come out in key order rather than the backing map's iteration
/// order: a `HashMap` walk means two identical requests can answer with
/// the rows in different orders, which a client diffing a listing sees
/// as spurious churn.
#[allow(clippy::too_many_arguments)]
pub(super) fn list_extras_filtered_xml(
    svc: &RdsService,
    aid: &str,
    category: &str,
    wrapper: &str,
    member_tag: &str,
    action: &str,
    keep: impl Fn(&Value) -> bool,
    render: impl Fn(&Value) -> String,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state_handle().read();
    let items: Vec<Value> = accounts
        .get(aid)
        .and_then(|s| s.extras.get(category))
        .map(|m| {
            let mut entries: Vec<(&String, &Value)> = m.iter().collect();
            entries.sort_by_key(|(key, _)| *key);
            entries
                .into_iter()
                .map(|(_, v)| v)
                .filter(|v| keep(v))
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let body = items
        .iter()
        .map(|v| {
            format!(
                "        <{member_tag}>\n{}\n        </{member_tag}>",
                render(v)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let inner = format!("    <{wrapper}>\n{body}\n    </{wrapper}>");
    Ok(xml_response(action, inner, rid))
}

/// Like [`list_extras_xml`] but wraps each element in the list's *named*
/// member tag (e.g. `<GlobalCluster>`) instead of the generic `<member>`.
/// RDS query-protocol Describe lists use the named member tag, and the AWS
/// SDK unmarshals an empty list when it sees `<member>` — which makes the
/// Terraform provider nil-deref on read. Also filters by a single id field
/// and raises `not_found_code` when a requested id is absent, matching AWS.
#[allow(clippy::too_many_arguments)]
pub(super) fn list_extras_named_xml(
    svc: &RdsService,
    aid: &str,
    category: &str,
    wrapper: &str,
    member_tag: &str,
    action: &str,
    render: impl Fn(&Value) -> String,
    wanted: Option<&str>,
    id_field: &str,
    not_found_code: &str,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state_handle().read();
    let items: Vec<Value> = accounts
        .get(aid)
        .and_then(|s| s.extras.get(category))
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default();
    if let Some(w) = wanted {
        if !items.iter().any(|v| v[id_field].as_str() == Some(w)) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                not_found_code,
                format!("{w} not found."),
            ));
        }
    }
    let body = items
        .iter()
        .filter(|v| wanted.is_none_or(|w| v[id_field].as_str() == Some(w)))
        .map(|v| {
            format!(
                "        <{member_tag}>\n{}\n        </{member_tag}>",
                render(v)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    Ok(xml_response(
        action,
        format!("    <{wrapper}>\n{body}\n    </{wrapper}>"),
        rid,
    ))
}
