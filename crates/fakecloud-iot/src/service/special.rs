//! Resource-specific handlers that the generic engine cannot express: minting
//! certificates and key pairs, deterministic endpoints, the CA registration
//! code, account-scoped singleton configurations (indexing / event / audit /
//! encryption / package / logging / default-authorizer), ARN-keyed tagging,
//! relationship operations (principal / policy attachments and thing-group /
//! billing-group membership), and the bounded fleet-index search.

use std::collections::HashMap;

use http::{HeaderMap, StatusCode};
use serde_json::{json, Map, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::generated::{OpMeta, Src};
use crate::state::IotData;

use super::{mint_arn, mint_hex64, ok_json, query_get, Ctx, IotService};

type Handled = Result<Option<(AwsResponse, bool)>, AwsServiceError>;

/// A label value as `&str` (labels are stored as `String`).
fn lbl<'a>(labels: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    labels.get(key).map(String::as_str)
}

/// The account-scoped singleton configuration a single-value op reads/writes.
enum Singleton {
    Get(&'static str),
    Set(&'static str),
    Delete(&'static str),
}

fn singleton_spec(op: &str) -> Option<Singleton> {
    Some(match op {
        "GetIndexingConfiguration" => Singleton::Get("indexing"),
        "UpdateIndexingConfiguration" => Singleton::Set("indexing"),
        "DescribeEventConfigurations" => Singleton::Get("eventconfig"),
        "UpdateEventConfigurations" => Singleton::Set("eventconfig"),
        "DescribeAccountAuditConfiguration" => Singleton::Get("auditconfig"),
        "UpdateAccountAuditConfiguration" => Singleton::Set("auditconfig"),
        "DeleteAccountAuditConfiguration" => Singleton::Delete("auditconfig"),
        "DescribeEncryptionConfiguration" => Singleton::Get("encryptionconfig"),
        "UpdateEncryptionConfiguration" => Singleton::Set("encryptionconfig"),
        "GetPackageConfiguration" => Singleton::Get("packageconfig"),
        "UpdatePackageConfiguration" => Singleton::Set("packageconfig"),
        "GetV2LoggingOptions" => Singleton::Get("v2logging"),
        "SetV2LoggingOptions" => Singleton::Set("v2logging"),
        "GetLoggingOptions" => Singleton::Get("logging"),
        "SetLoggingOptions" => Singleton::Set("logging"),
        "DescribeDefaultAuthorizer" => Singleton::Get("defaultauth"),
        "SetDefaultAuthorizer" => Singleton::Set("defaultauth"),
        "ClearDefaultAuthorizer" => Singleton::Delete("defaultauth"),
        _ => return None,
    })
}

pub(super) fn dispatch(
    svc: &IotService,
    meta: &'static OpMeta,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    query: &[(String, String)],
    headers: &HeaderMap,
    body: &Map<String, Value>,
) -> Handled {
    // Singleton configurations.
    if let Some(spec) = singleton_spec(meta.op) {
        return Ok(Some(handle_singleton(svc, meta, ctx, spec, body)));
    }

    match meta.op {
        "CreateKeysAndCertificate" => Ok(Some(create_keys_and_certificate(svc, ctx, query))),
        "CreateCertificateFromCsr" => Ok(Some(create_cert_from_csr(svc, ctx, query))),
        "RegisterCertificate" | "RegisterCertificateWithoutCA" => {
            Ok(Some(register_certificate(svc, ctx, body, "certificates")))
        }
        "RegisterCACertificate" => Ok(Some(register_certificate(svc, ctx, body, "cacertificates"))),
        "DescribeEndpoint" => Ok(Some(describe_endpoint(ctx, query))),
        "GetRegistrationCode" => Ok(Some(get_registration_code(ctx))),

        "TagResource" => Ok(Some(tag_resource(svc, ctx, body))),
        "UntagResource" => Ok(Some(untag_resource(svc, ctx, body))),
        "ListTagsForResource" => Ok(Some(list_tags(svc, ctx, query))),

        // Principal <-> thing.
        "AttachThingPrincipal" => Ok(Some(relate(
            svc,
            ctx,
            true,
            &[(
                "thing-principals",
                lbl(labels, "thingName"),
                header_principal(meta, headers),
            )],
        ))),
        "DetachThingPrincipal" => Ok(Some(relate(
            svc,
            ctx,
            false,
            &[(
                "thing-principals",
                lbl(labels, "thingName"),
                header_principal(meta, headers),
            )],
        ))),
        "ListThingPrincipals" => Ok(Some(list_relation(
            svc,
            ctx,
            "thing-principals",
            lbl(labels, "thingName"),
            "principals",
        ))),
        "ListThingPrincipalsV2" => Ok(Some(list_relation_objs(
            svc,
            ctx,
            "thing-principals",
            lbl(labels, "thingName"),
            "thingPrincipalObjects",
            "principal",
        ))),

        // Principal <-> policy (cert/identity principals).
        "AttachPrincipalPolicy" => Ok(Some(relate(
            svc,
            ctx,
            true,
            &[(
                "principal-policies",
                header_principal(meta, headers),
                lbl(labels, "policyName"),
            )],
        ))),
        "DetachPrincipalPolicy" => Ok(Some(relate(
            svc,
            ctx,
            false,
            &[(
                "principal-policies",
                header_principal(meta, headers),
                lbl(labels, "policyName"),
            )],
        ))),
        "ListPrincipalPolicies" => Ok(Some(list_relation_policies(
            svc,
            ctx,
            "principal-policies",
            header_principal(meta, headers),
        ))),

        // Policy <-> target (v2 policy attachment).
        "AttachPolicy" => Ok(Some(relate(
            svc,
            ctx,
            true,
            &[(
                "policy-targets",
                lbl(labels, "policyName"),
                body_str(body, "target"),
            )],
        ))),
        "DetachPolicy" => Ok(Some(relate(
            svc,
            ctx,
            false,
            &[(
                "policy-targets",
                lbl(labels, "policyName"),
                body_str(body, "target"),
            )],
        ))),
        "ListTargetsForPolicy" => Ok(Some(list_relation(
            svc,
            ctx,
            "policy-targets",
            lbl(labels, "policyName"),
            "targets",
        ))),
        "ListAttachedPolicies" => Ok(Some(list_attached_policies(
            svc,
            ctx,
            lbl(labels, "target"),
        ))),

        // Thing <-> thing group.
        "AddThingToThingGroup" => Ok(Some(relate(
            svc,
            ctx,
            true,
            &[(
                "group-things",
                body_str(body, "thingGroupName"),
                body_str(body, "thingName"),
            )],
        ))),
        "RemoveThingFromThingGroup" => Ok(Some(relate(
            svc,
            ctx,
            false,
            &[(
                "group-things",
                body_str(body, "thingGroupName"),
                body_str(body, "thingName"),
            )],
        ))),
        "ListThingsInThingGroup" => Ok(Some(list_relation(
            svc,
            ctx,
            "group-things",
            lbl(labels, "thingGroupName"),
            "things",
        ))),

        // Thing <-> billing group.
        "AddThingToBillingGroup" => Ok(Some(relate(
            svc,
            ctx,
            true,
            &[(
                "billing-things",
                body_str(body, "billingGroupName"),
                body_str(body, "thingName"),
            )],
        ))),
        "RemoveThingFromBillingGroup" => Ok(Some(relate(
            svc,
            ctx,
            false,
            &[(
                "billing-things",
                body_str(body, "billingGroupName"),
                body_str(body, "thingName"),
            )],
        ))),
        "ListThingsInBillingGroup" => Ok(Some(list_relation(
            svc,
            ctx,
            "billing-things",
            lbl(labels, "billingGroupName"),
            "things",
        ))),

        "SearchIndex" => Ok(Some(search_index(svc, ctx, meta, body)?)),

        // Topic rules: the rule payload is the request body (`@httpPayload`);
        // GetTopicRule nests it under `rule` alongside the `ruleArn`.
        "CreateTopicRule" | "ReplaceTopicRule" => Ok(Some(put_topic_rule(svc, ctx, labels, body))),
        "GetTopicRule" => Ok(Some(get_topic_rule(svc, ctx, meta, labels)?)),
        "EnableTopicRule" => Ok(Some(set_topic_rule_disabled(svc, ctx, labels, false))),
        "DisableTopicRule" => Ok(Some(set_topic_rule_disabled(svc, ctx, labels, true))),

        _ => Ok(None),
    }
}

// ---------- certificates ----------

fn placeholder_pem(kind: &str, id: &str) -> String {
    // A structurally-shaped, self-signed-looking PEM placeholder (not a real
    // CA-signed X.509 chain). Deterministic from the certificate id.
    let seed = super::mint_hex64(&format!("{kind}:{id}"));
    format!(
        "-----BEGIN {kind}-----\nMIIB{}\n-----END {kind}-----\n",
        &seed
    )
}

fn key_pair(id: &str) -> Value {
    json!({
        "PublicKey": format!(
            "-----BEGIN PUBLIC KEY-----\n{}\n-----END PUBLIC KEY-----\n",
            mint_hex64(&format!("pub:{id}"))
        ),
        "PrivateKey": format!(
            "-----BEGIN RSA PRIVATE KEY-----\n{}\n-----END RSA PRIVATE KEY-----\n",
            mint_hex64(&format!("priv:{id}"))
        ),
    })
}

fn store_certificate(
    svc: &IotService,
    ctx: &Ctx,
    rtype: &str,
    cert_id: &str,
    active: bool,
) -> String {
    let arn = mint_arn(ctx, rtype, cert_id);
    let record = json!({
        "certificateId": cert_id,
        "certificateArn": arn,
        "status": if active { "ACTIVE" } else { "INACTIVE" },
        "certificatePem": placeholder_pem("CERTIFICATE", cert_id),
        "creationDate": super::now_iso(),
        "caCertificateId": Value::Null,
    });
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.put_resource(rtype, cert_id, record);
    arn
}

fn create_keys_and_certificate(
    svc: &IotService,
    ctx: &Ctx,
    query: &[(String, String)],
) -> (AwsResponse, bool) {
    let seq = { svc.state.write().get_or_create(&ctx.account).next_seq() };
    let cert_id = mint_hex64(&format!("{}:cert:{seq}", ctx.account));
    let active = query_get(query, "setAsActive") != Some("false");
    let arn = store_certificate(svc, ctx, "certificates", &cert_id, active);
    (
        ok_json(json!({
            "certificateArn": arn,
            "certificateId": cert_id,
            "certificatePem": placeholder_pem("CERTIFICATE", &cert_id),
            "keyPair": key_pair(&cert_id),
        })),
        true,
    )
}

fn create_cert_from_csr(
    svc: &IotService,
    ctx: &Ctx,
    query: &[(String, String)],
) -> (AwsResponse, bool) {
    let seq = { svc.state.write().get_or_create(&ctx.account).next_seq() };
    let cert_id = mint_hex64(&format!("{}:csr:{seq}", ctx.account));
    let active = query_get(query, "setAsActive") == Some("true");
    let arn = store_certificate(svc, ctx, "certificates", &cert_id, active);
    (
        ok_json(json!({
            "certificateArn": arn,
            "certificateId": cert_id,
            "certificatePem": placeholder_pem("CERTIFICATE", &cert_id),
        })),
        true,
    )
}

fn register_certificate(
    svc: &IotService,
    ctx: &Ctx,
    body: &Map<String, Value>,
    rtype: &str,
) -> (AwsResponse, bool) {
    let pem = body
        .get("certificatePem")
        .and_then(Value::as_str)
        .unwrap_or("");
    let seq = { svc.state.write().get_or_create(&ctx.account).next_seq() };
    let cert_id = mint_hex64(&format!("{}:reg:{}:{seq}", ctx.account, pem));
    let arn = store_certificate(svc, ctx, rtype, &cert_id, false);
    (
        ok_json(json!({ "certificateArn": arn, "certificateId": cert_id })),
        true,
    )
}

// ---------- endpoint + registration code ----------

fn describe_endpoint(ctx: &Ctx, query: &[(String, String)]) -> (AwsResponse, bool) {
    let endpoint_type = query_get(query, "endpointType").unwrap_or("iot:Data-ATS");
    let prefix = &super::mint_hex64(&ctx.account)[..14];
    let host = match endpoint_type {
        "iot:CredentialProvider" => {
            format!("{prefix}.credentials.iot.{}.amazonaws.com", ctx.region)
        }
        "iot:Jobs" => format!("{prefix}.jobs.iot.{}.amazonaws.com", ctx.region),
        _ => format!("{prefix}-ats.iot.{}.amazonaws.com", ctx.region),
    };
    (ok_json(json!({ "endpointAddress": host })), false)
}

fn get_registration_code(ctx: &Ctx) -> (AwsResponse, bool) {
    (
        ok_json(json!({ "registrationCode": mint_hex64(&format!("regcode:{}", ctx.account)) })),
        false,
    )
}

// ---------- singletons ----------

fn handle_singleton(
    svc: &IotService,
    meta: &OpMeta,
    ctx: &Ctx,
    spec: Singleton,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    match spec {
        Singleton::Get(key) => {
            let g = svc.state.read();
            let stored = g
                .get(&ctx.account)
                .and_then(|d| d.singletons.get(key))
                .cloned()
                .unwrap_or_else(|| Value::Object(Map::new()));
            (ok_json(super::build_output(meta, &stored)), false)
        }
        Singleton::Set(key) => {
            let mut g = svc.state.write();
            let data = g.get_or_create(&ctx.account);
            data.singletons
                .insert(key.to_string(), Value::Object(body.clone()));
            (
                ok_json(super::build_output(meta, &Value::Object(body.clone()))),
                true,
            )
        }
        Singleton::Delete(key) => {
            let mut g = svc.state.write();
            let data = g.get_or_create(&ctx.account);
            data.singletons.remove(key);
            (ok_json(Value::Object(Map::new())), true)
        }
    }
}

// ---------- tags ----------

fn tag_resource(svc: &IotService, ctx: &Ctx, body: &Map<String, Value>) -> (AwsResponse, bool) {
    let arn = body_str(body, "resourceArn").unwrap_or("").to_string();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let entry = data.tags.entry(arn).or_default();
    if let Some(Value::Array(tags)) = body.get("tags") {
        for t in tags {
            let k = t
                .get("Key")
                .or_else(|| t.get("key"))
                .and_then(Value::as_str);
            let v = t
                .get("Value")
                .or_else(|| t.get("value"))
                .and_then(Value::as_str);
            if let Some(k) = k {
                entry.insert(k.to_string(), v.unwrap_or("").to_string());
            }
        }
    }
    (ok_json(Value::Object(Map::new())), true)
}

fn untag_resource(svc: &IotService, ctx: &Ctx, body: &Map<String, Value>) -> (AwsResponse, bool) {
    let arn = body_str(body, "resourceArn").unwrap_or("").to_string();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if let Some(entry) = data.tags.get_mut(&arn) {
        if let Some(Value::Array(keys)) = body.get("tagKeys") {
            for k in keys {
                if let Some(k) = k.as_str() {
                    entry.remove(k);
                }
            }
        }
    }
    (ok_json(Value::Object(Map::new())), true)
}

fn list_tags(svc: &IotService, ctx: &Ctx, query: &[(String, String)]) -> (AwsResponse, bool) {
    let arn = query_get(query, "resourceArn").unwrap_or("");
    let g = svc.state.read();
    let tags: Vec<Value> = g
        .get(&ctx.account)
        .and_then(|d| d.tags.get(arn))
        .map(|m| {
            m.iter()
                .map(|(k, v)| json!({ "Key": k, "Value": v }))
                .collect()
        })
        .unwrap_or_default();
    (ok_json(json!({ "tags": tags })), false)
}

// ---------- relationships ----------

/// The principal value for a request: read the operation's single header-bound
/// member (e.g. `x-amzn-principal` / `x-amzn-iot-principal`) by its model wire
/// name.
fn header_principal<'a>(meta: &OpMeta, headers: &'a HeaderMap) -> Option<&'a str> {
    let rule = meta.rules.iter().find(|r| matches!(r.src, Src::Header))?;
    headers.get(rule.wire).and_then(|h| h.to_str().ok())
}

fn body_str<'a>(body: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    body.get(key).and_then(Value::as_str)
}

/// Add or remove a set of `(relation, a, b)` edges. Each edge stores `b` under
/// `relation:a`. A `None` endpoint is a no-op (validation already ran).
fn relate(
    svc: &IotService,
    ctx: &Ctx,
    add: bool,
    edges: &[(&str, Option<&str>, Option<&str>)],
) -> (AwsResponse, bool) {
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let mut mutated = false;
    for (rel, a, b) in edges {
        let (Some(a), Some(b)) = (a, b) else { continue };
        let key = format!("{rel}:{a}");
        let entry = data.relations.entry(key).or_default();
        if add {
            if !entry.iter().any(|x| x == b) {
                entry.push((*b).to_string());
                mutated = true;
            }
        } else if let Some(pos) = entry.iter().position(|x| x == b) {
            entry.remove(pos);
            mutated = true;
        }
    }
    (ok_json(Value::Object(Map::new())), mutated)
}

fn relation_values(data: Option<&IotData>, rel: &str, a: Option<&str>) -> Vec<String> {
    let Some(a) = a else { return Vec::new() };
    data.and_then(|d| d.relations.get(&format!("{rel}:{a}")))
        .cloned()
        .unwrap_or_default()
}

fn list_relation(
    svc: &IotService,
    ctx: &Ctx,
    rel: &str,
    a: Option<&str>,
    field: &str,
) -> (AwsResponse, bool) {
    let g = svc.state.read();
    let values = relation_values(g.get(&ctx.account), rel, a);
    (ok_json(json!({ field: values })), false)
}

fn list_relation_objs(
    svc: &IotService,
    ctx: &Ctx,
    rel: &str,
    a: Option<&str>,
    field: &str,
    inner: &str,
) -> (AwsResponse, bool) {
    let g = svc.state.read();
    let values = relation_values(g.get(&ctx.account), rel, a);
    let objs: Vec<Value> = values.into_iter().map(|v| json!({ inner: v })).collect();
    (ok_json(json!({ field: objs })), false)
}

fn list_relation_policies(
    svc: &IotService,
    ctx: &Ctx,
    rel: &str,
    a: Option<&str>,
) -> (AwsResponse, bool) {
    let g = svc.state.read();
    let values = relation_values(g.get(&ctx.account), rel, a);
    let policies: Vec<Value> = values
        .into_iter()
        .map(|name| {
            let arn = mint_arn(ctx, "policies", &name);
            json!({ "policyName": name, "policyArn": arn })
        })
        .collect();
    (ok_json(json!({ "policies": policies })), false)
}

fn list_attached_policies(
    svc: &IotService,
    ctx: &Ctx,
    target: Option<&str>,
) -> (AwsResponse, bool) {
    // Attached policies for a target are the inverse of `policy-targets`.
    let g = svc.state.read();
    let data = g.get(&ctx.account);
    let mut policies = Vec::new();
    if let (Some(target), Some(data)) = (target, data) {
        for (key, targets) in &data.relations {
            if let Some(policy) = key.strip_prefix("policy-targets:") {
                if targets.iter().any(|t| t == target) {
                    let arn = mint_arn(ctx, "policies", policy);
                    policies.push(json!({ "policyName": policy, "policyArn": arn }));
                }
            }
        }
    }
    (ok_json(json!({ "policies": policies })), false)
}

// ---------- topic rules ----------

fn put_topic_rule(
    svc: &IotService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let Some(name) = labels.get("ruleName") else {
        return (ok_json(Value::Object(Map::new())), false);
    };
    let existing = {
        let g = svc.state.read();
        g.get(&ctx.account)
            .and_then(|d| d.get_resource("rules", name))
            .cloned()
    };
    let mut record = body.clone();
    record.insert("ruleName".to_string(), Value::String(name.clone()));
    record.insert(
        "ruleArn".to_string(),
        Value::String(mint_arn(ctx, "rules", name)),
    );
    record
        .entry("createdAt")
        .or_insert_with(|| Value::String(super::now_iso()));
    // Preserve the disabled flag across a replace; default to enabled.
    let disabled = existing
        .as_ref()
        .and_then(|e| e.get("ruleDisabled"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    record
        .entry("ruleDisabled")
        .or_insert_with(|| Value::Bool(disabled));
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.put_resource("rules", name, Value::Object(record));
    (ok_json(Value::Object(Map::new())), true)
}

fn get_topic_rule(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = labels.get("ruleName").cloned().unwrap_or_default();
    let g = svc.state.read();
    let record = g
        .get(&ctx.account)
        .and_then(|d| d.get_resource("rules", &name).cloned());
    match record {
        Some(mut rec) => {
            let arn = rec
                .as_object_mut()
                .and_then(|o| o.remove("ruleArn"))
                .unwrap_or_else(|| Value::String(mint_arn(ctx, "rules", &name)));
            Ok((ok_json(json!({ "rule": rec, "ruleArn": arn })), false))
        }
        None => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            crate::validate::validation_error_code(meta),
            format!("Rule '{name}' does not exist."),
        )),
    }
}

fn set_topic_rule_disabled(
    svc: &IotService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    disabled: bool,
) -> (AwsResponse, bool) {
    let name = labels.get("ruleName").cloned().unwrap_or_default();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if let Some(rec) = data
        .resources
        .get_mut("rules")
        .and_then(|m| m.get_mut(&name))
    {
        if let Some(o) = rec.as_object_mut() {
            o.insert("ruleDisabled".to_string(), Value::Bool(disabled));
        }
    }
    (ok_json(Value::Object(Map::new())), true)
}

// ---------- fleet index search ----------

fn search_index(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let query = body_str(body, "queryString")
        .unwrap_or("")
        .trim()
        .to_string();
    let g = svc.state.read();
    let things = g
        .get(&ctx.account)
        .map(|d| d.list_resources("things"))
        .unwrap_or_default();

    // Bounded query subset: `*` (all things), `thingName:NAME`, or a bare name.
    let matched: Vec<Value> = if query == "*" || query.is_empty() {
        things
    } else if let Some(name) = query.strip_prefix("thingName:") {
        let name = name.trim();
        things
            .into_iter()
            .filter(|t| t.get("thingName").and_then(Value::as_str) == Some(name))
            .collect()
    } else if !query.contains(':') && !query.contains(' ') {
        things
            .into_iter()
            .filter(|t| t.get("thingName").and_then(Value::as_str) == Some(query.as_str()))
            .collect()
    } else {
        let code = if meta.errors.contains(&"InvalidQueryException") {
            "InvalidQueryException"
        } else {
            crate::validate::validation_error_code(meta)
        };
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            code,
            format!("The query '{query}' is not supported by fakecloud's bounded fleet index."),
        ));
    };

    let docs: Vec<Value> = matched
        .into_iter()
        .map(|t| {
            let mut m = Map::new();
            if let Some(n) = t.get("thingName") {
                m.insert("thingName".to_string(), n.clone());
            }
            if let Some(id) = t.get("thingId") {
                m.insert("thingId".to_string(), id.clone());
            }
            Value::Object(m)
        })
        .collect();
    Ok((ok_json(json!({ "things": docs })), false))
}
