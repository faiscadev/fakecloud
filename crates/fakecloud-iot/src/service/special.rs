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

        // Security profiles: the list returns identifier objects `{name, arn}`
        // projected from the stored profile (whose fields are named
        // `securityProfileName` / `securityProfileArn`).
        "ListSecurityProfiles" => Ok(Some(list_security_profiles(svc, ctx))),

        // Audit suppressions are body-keyed (checkName + resourceIdentifier)
        // rather than URI-addressed, so they need explicit persistence.
        "CreateAuditSuppression" => Ok(Some(put_audit_suppression(svc, ctx, body, false))),
        "UpdateAuditSuppression" => Ok(Some(put_audit_suppression(svc, ctx, body, true))),
        "DescribeAuditSuppression" => Ok(Some(describe_audit_suppression(svc, ctx, meta, body)?)),
        "DeleteAuditSuppression" => Ok(Some(delete_audit_suppression(svc, ctx, body))),
        "ListAuditSuppressions" => Ok(Some(list_audit_suppressions(svc, ctx))),

        // DescribeJob returns `documentSource` as a top-level member alongside
        // the `job` wrapper (the model places it outside the Job struct).
        "DescribeJob" => Ok(Some(describe_job(svc, ctx, meta, labels)?)),

        // Jobs: document read + lifecycle mutations that echo the job identity.
        "GetJobDocument" => Ok(Some(get_job_document(svc, ctx, meta, labels)?)),
        "AssociateTargetsWithJob" => Ok(Some(job_lifecycle(svc, ctx, meta, labels, body, false)?)),
        "CancelJob" => Ok(Some(job_lifecycle(svc, ctx, meta, labels, body, true)?)),

        // Managed IAM-style policy versioning. Versions live in their own
        // `policy-versions` store keyed `policyName/versionId` so the policy
        // list is not polluted; CreatePolicy seeds version 1 as the default.
        "CreatePolicy" => Ok(Some(create_policy(svc, ctx, labels, body)?)),
        "CreatePolicyVersion" => Ok(Some(create_policy_version(
            svc, ctx, meta, labels, query, body,
        )?)),
        "GetPolicyVersion" => Ok(Some(get_policy_version(svc, ctx, meta, labels)?)),
        "ListPolicyVersions" => Ok(Some(list_policy_versions(svc, ctx, meta, labels)?)),
        "SetDefaultPolicyVersion" => Ok(Some(set_default_policy_version(svc, ctx, meta, labels)?)),
        "DeletePolicyVersion" => Ok(Some(delete_policy_version(svc, ctx, meta, labels)?)),

        // Provisioning templates: persist the template + its versions so the
        // Describe / List reads (generic and version-specific) round-trip, and
        // mint a real (persisted) certificate for the provisioning claim.
        "CreateProvisioningTemplate" => Ok(Some(create_provisioning_template(svc, ctx, body)?)),
        "CreateProvisioningTemplateVersion" => Ok(Some(create_provisioning_template_version(
            svc, ctx, meta, labels, query, body,
        )?)),
        "DescribeProvisioningTemplateVersion" => Ok(Some(describe_provisioning_template_version(
            svc, ctx, meta, labels,
        )?)),
        "ListProvisioningTemplateVersions" => Ok(Some(list_provisioning_template_versions(
            svc, ctx, meta, labels,
        )?)),
        "CreateProvisioningClaim" => Ok(Some(create_provisioning_claim(svc, ctx, meta, labels)?)),

        // Certificate transfer + fleet-provisioning thing registration.
        "TransferCertificate" => Ok(Some(transfer_certificate(
            svc, ctx, meta, labels, query, body,
        )?)),
        "RegisterThing" => Ok(Some(register_thing(svc, ctx))),

        // Long-running tasks whose only synchronous output is a minted taskId;
        // persist a task record so the matching Describe read round-trips.
        "StartThingRegistrationTask"
        | "StartOnDemandAuditTask"
        | "StartAuditMitigationActionsTask"
        | "StartDetectMitigationActionsTask" => Ok(Some(start_task(svc, ctx, meta, labels, body))),

        // Principal <-> thing and thing <-> group listings are the inverse of
        // the stored relation edges (mirroring ListAttachedPolicies).
        "ListPrincipalThings" => Ok(Some(list_principal_things(svc, ctx, meta, headers, false))),
        "ListPrincipalThingsV2" => Ok(Some(list_principal_things(svc, ctx, meta, headers, true))),
        "ListThingGroupsForThing" => Ok(Some(list_thing_groups_for_thing(svc, ctx, labels))),

        // Topic rules: the rule payload is the request body (`@httpPayload`);
        // GetTopicRule nests it under `rule` alongside the `ruleArn`.
        "CreateTopicRule" | "ReplaceTopicRule" => Ok(Some(put_topic_rule(svc, ctx, labels, body))),
        "GetTopicRule" => Ok(Some(get_topic_rule(svc, ctx, meta, labels)?)),
        "EnableTopicRule" => Ok(Some(set_topic_rule_disabled(svc, ctx, labels, false))),
        "DisableTopicRule" => Ok(Some(set_topic_rule_disabled(svc, ctx, labels, true))),

        // Topic-rule destinations: the arn is server-minted (nlabels:0 create),
        // so the generic engine can't key it. Store into the shared
        // `destinations` resource family that Get/List/Delete read.
        "CreateTopicRuleDestination" => Ok(Some(create_topic_rule_destination(svc, ctx, body))),
        "UpdateTopicRuleDestination" => Ok(Some(update_topic_rule_destination(svc, ctx, body))),
        // No async confirmation channel exists, so destinations are created
        // ENABLED; Confirm is accepted idempotently (AWS returns an empty body).
        "ConfirmTopicRuleDestination" => Ok(Some((ok_json(Value::Object(Map::new())), false))),

        // Bulk thing<->group membership; mirrors AddThingToThingGroup's
        // `group-things` relation so ListThingGroupsForThing reflects it.
        "UpdateThingGroupsForThing" => Ok(Some(update_thing_groups_for_thing(svc, ctx, body))),

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
        seed
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
        "creationDate": super::now_epoch(),
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

fn create_topic_rule_destination(
    svc: &IotService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let cfg = body.get("destinationConfiguration");
    let http = cfg.and_then(|c| c.get("httpUrlConfiguration"));
    let vpc = cfg.and_then(|c| c.get("vpcConfiguration"));
    let http_props = http
        .and_then(|h| h.get("confirmationUrl"))
        .map(|u| json!({ "confirmationUrl": u }));

    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    data.seq += 1;
    let kind = if vpc.is_some() { "vpc" } else { "http" };
    let uid = super::mint_uuid(&format!("{}:dest:{}", ctx.account, data.seq));
    let arn = format!(
        "arn:aws:iot:{}:{}:ruledestination/{kind}/{uid}",
        ctx.region, ctx.account
    );
    let now = super::now_epoch();

    let mut dest = Map::new();
    dest.insert("arn".into(), Value::String(arn.clone()));
    dest.insert("status".into(), Value::String("ENABLED".into()));
    dest.insert("createdAt".into(), now.clone());
    dest.insert("lastUpdatedAt".into(), now.clone());
    if let Some(hp) = &http_props {
        dest.insert("httpUrlProperties".into(), hp.clone());
    }
    if let Some(v) = vpc {
        dest.insert("vpcProperties".into(), v.clone());
    }
    let destination = Value::Object(dest);

    // Record: top-level fields feed the List element projection; the nested
    // `topicRuleDestination` feeds the Get struct projection.
    let mut record = Map::new();
    record.insert("arn".into(), Value::String(arn.clone()));
    record.insert("status".into(), Value::String("ENABLED".into()));
    record.insert("createdAt".into(), now.clone());
    record.insert("lastUpdatedAt".into(), now);
    if let Some(hp) = &http_props {
        record.insert("httpUrlSummary".into(), hp.clone());
    }
    record.insert("topicRuleDestination".into(), destination.clone());
    data.put_resource("destinations", &arn, Value::Object(record));

    (
        ok_json(json!({ "topicRuleDestination": destination })),
        true,
    )
}

fn update_topic_rule_destination(
    svc: &IotService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let Some(arn) = body_str(body, "arn") else {
        return (ok_json(Value::Object(Map::new())), false);
    };
    let status = body_str(body, "status").unwrap_or("ENABLED").to_string();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let Some(Value::Object(mut record)) = data.get_resource("destinations", arn).cloned() else {
        return (ok_json(Value::Object(Map::new())), false);
    };
    record.insert("status".into(), Value::String(status.clone()));
    record.insert("lastUpdatedAt".into(), super::now_epoch());
    if let Some(Value::Object(dest)) = record.get_mut("topicRuleDestination") {
        dest.insert("status".into(), Value::String(status));
    }
    data.put_resource("destinations", arn, Value::Object(record));
    (ok_json(Value::Object(Map::new())), true)
}

fn update_thing_groups_for_thing(
    svc: &IotService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let Some(thing) = body_str(body, "thingName") else {
        return (ok_json(Value::Object(Map::new())), false);
    };
    let adds: Vec<String> = body
        .get("thingGroupsToAdd")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let removes: Vec<String> = body
        .get("thingGroupsToRemove")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let mut mutated = false;
    for grp in &adds {
        let entry = data
            .relations
            .entry(format!("group-things:{grp}"))
            .or_default();
        if !entry.iter().any(|x| x == thing) {
            entry.push(thing.to_string());
            mutated = true;
        }
    }
    for grp in &removes {
        if let Some(entry) = data.relations.get_mut(&format!("group-things:{grp}")) {
            if let Some(pos) = entry.iter().position(|x| x == thing) {
                entry.remove(pos);
                mutated = true;
            }
        }
    }
    (ok_json(Value::Object(Map::new())), mutated)
}

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
    record.entry("createdAt").or_insert_with(super::now_epoch);
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

// ---------- security profiles ----------

fn list_security_profiles(svc: &IotService, ctx: &Ctx) -> (AwsResponse, bool) {
    let g = svc.state.read();
    let records = g
        .get(&ctx.account)
        .map(|d| d.list_resources("security-profiles"))
        .unwrap_or_default();
    let identifiers: Vec<Value> = records
        .into_iter()
        .filter_map(|r| {
            let name = r.get("securityProfileName").and_then(Value::as_str)?;
            let arn = r.get("securityProfileArn").and_then(Value::as_str)?;
            Some(json!({ "name": name, "arn": arn }))
        })
        .collect();
    (
        ok_json(json!({ "securityProfileIdentifiers": identifiers })),
        false,
    )
}

// ---------- audit suppressions ----------

/// Audit suppressions are identified by `checkName` + `resourceIdentifier`
/// (both in the request body), not by a URI label. This builds a stable
/// storage key from those two fields, canonicalising the `resourceIdentifier`
/// object so key ordering does not matter across create / describe / delete.
fn suppression_key(body: &Map<String, Value>) -> String {
    let check = body_str(body, "checkName").unwrap_or("");
    let ri = body
        .get("resourceIdentifier")
        .map(canonical_json)
        .unwrap_or_default();
    format!("{check}\u{1f}{ri}")
}

fn canonical_json(v: &Value) -> String {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let parts: Vec<String> = keys
                .iter()
                .map(|k| format!("{k}={}", canonical_json(&m[*k])))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// The declared members of an audit suppression (create/update input and
/// describe/list output share these).
const SUPPRESSION_FIELDS: &[&str] = &[
    "checkName",
    "resourceIdentifier",
    "expirationDate",
    "suppressIndefinitely",
    "description",
];

fn put_audit_suppression(
    svc: &IotService,
    ctx: &Ctx,
    body: &Map<String, Value>,
    is_update: bool,
) -> (AwsResponse, bool) {
    let key = suppression_key(body);
    let mut record = Map::new();
    for field in SUPPRESSION_FIELDS {
        if let Some(v) = body.get(*field) {
            record.insert((*field).to_string(), v.clone());
        }
    }
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if is_update {
        if let Some(existing) = data
            .get_resource("audit-suppressions", &key)
            .and_then(Value::as_object)
            .cloned()
        {
            let mut merged = existing;
            for (k, v) in record {
                merged.insert(k, v);
            }
            record = merged;
        }
    }
    data.put_resource("audit-suppressions", &key, Value::Object(record));
    (ok_json(Value::Object(Map::new())), true)
}

fn describe_audit_suppression(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let key = suppression_key(body);
    let g = svc.state.read();
    match g
        .get(&ctx.account)
        .and_then(|d| d.get_resource("audit-suppressions", &key))
        .cloned()
    {
        Some(record) => Ok((ok_json(record), false)),
        None => Err(super::engine::not_found(meta, &key)),
    }
}

fn delete_audit_suppression(
    svc: &IotService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let key = suppression_key(body);
    let mut g = svc.state.write();
    g.get_or_create(&ctx.account)
        .remove_resource("audit-suppressions", &key);
    (ok_json(Value::Object(Map::new())), true)
}

fn list_audit_suppressions(svc: &IotService, ctx: &Ctx) -> (AwsResponse, bool) {
    let g = svc.state.read();
    let suppressions = g
        .get(&ctx.account)
        .map(|d| d.list_resources("audit-suppressions"))
        .unwrap_or_default();
    (ok_json(json!({ "suppressions": suppressions })), false)
}

// ---------- jobs ----------

fn describe_job(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let job_id = labels.get("jobId").cloned().unwrap_or_default();
    let g = svc.state.read();
    match g
        .get(&ctx.account)
        .and_then(|d| d.get_resource("jobs", &job_id))
        .cloned()
    {
        Some(mut record) => {
            // `documentSource` is a top-level member of DescribeJobResponse,
            // outside the `job` wrapper; lift it out of the stored record.
            let document_source = record
                .as_object_mut()
                .and_then(|o| o.remove("documentSource"))
                .filter(|v| !v.is_null());
            let mut out = Map::new();
            if let Some(ds) = document_source {
                out.insert("documentSource".to_string(), ds);
            }
            out.insert("job".to_string(), record);
            Ok((ok_json(Value::Object(out)), false))
        }
        None => Err(super::engine::not_found(meta, &job_id)),
    }
}

fn get_job_document(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let job_id = labels.get("jobId").cloned().unwrap_or_default();
    let g = svc.state.read();
    match g
        .get(&ctx.account)
        .and_then(|d| d.get_resource("jobs", &job_id))
        .cloned()
    {
        Some(record) => {
            // CreateJob persists the inline `document` on the job record; echo it
            // back verbatim. A job created from a `documentSource` has no inline
            // document, so the member is simply absent (a shape-valid response).
            let mut out = Map::new();
            if let Some(doc) = record.get("document").filter(|v| v.is_string()) {
                out.insert("document".to_string(), doc.clone());
            }
            Ok((ok_json(Value::Object(out)), false))
        }
        None => Err(super::engine::not_found(meta, &job_id)),
    }
}

/// AssociateTargetsWithJob (append targets) and CancelJob (mark cancelled) both
/// mutate the stored job and echo its identity (`jobArn` / `jobId` /
/// `description`).
fn job_lifecycle(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
    body: &Map<String, Value>,
    cancel: bool,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let job_id = labels.get("jobId").cloned().unwrap_or_default();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let Some(mut record) = data.get_resource("jobs", &job_id).cloned() else {
        return Err(super::engine::not_found(meta, &job_id));
    };
    if let Some(obj) = record.as_object_mut() {
        if cancel {
            obj.insert("status".to_string(), Value::String("CANCELED".to_string()));
            for key in ["reasonCode", "comment"] {
                if let Some(v) = body.get(key) {
                    obj.insert(key.to_string(), v.clone());
                }
            }
        } else if let Some(Value::Array(added)) = body.get("targets") {
            let targets = obj
                .entry("targets")
                .or_insert_with(|| Value::Array(Vec::new()));
            if let Value::Array(existing) = targets {
                for t in added {
                    if !existing.contains(t) {
                        existing.push(t.clone());
                    }
                }
            }
        }
        obj.insert("lastUpdatedAt".to_string(), super::now_epoch());
    }
    let mut out = Map::new();
    out.insert("jobId".to_string(), Value::String(job_id.clone()));
    let arn = record
        .get("jobArn")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| mint_arn(ctx, "jobs", &job_id));
    out.insert("jobArn".to_string(), Value::String(arn));
    if let Some(desc) = record.get("description").filter(|v| v.is_string()) {
        out.insert("description".to_string(), desc.clone());
    }
    data.put_resource("jobs", &job_id, record);
    Ok((ok_json(Value::Object(out)), true))
}

// ---------- policies + versions ----------

fn policy_version_key(policy: &str, vid: &str) -> String {
    format!("{policy}/{vid}")
}

/// Persist one policy version under `policy-versions`, storing both the wire
/// name (`policyVersionId` / `creationDate`, used by GetPolicyVersion) and the
/// list-element name (`versionId` / `createDate`, used by ListPolicyVersions).
fn store_policy_version(
    data: &mut IotData,
    policy: &str,
    arn: &str,
    document: &str,
    vid: &str,
    is_default: bool,
) {
    let rec = json!({
        "policyName": policy,
        "policyArn": arn,
        "policyDocument": document,
        "policyVersionId": vid,
        "versionId": vid,
        "isDefaultVersion": is_default,
        "creationDate": super::now_epoch(),
        "createDate": super::now_epoch(),
        "lastModifiedDate": super::now_epoch(),
        "generationId": "1",
    });
    data.put_resource("policy-versions", &policy_version_key(policy, vid), rec);
}

/// The next monotonic numeric version id for a policy (max existing + 1).
fn next_policy_version_id(data: &IotData, policy: &str) -> String {
    let prefix = format!("{policy}/");
    let max = data
        .resources
        .get("policy-versions")
        .map(|m| {
            m.keys()
                .filter_map(|k| k.strip_prefix(&prefix))
                .filter_map(|s| s.parse::<u64>().ok())
                .max()
                .unwrap_or(0)
        })
        .unwrap_or(0);
    (max + 1).to_string()
}

fn clear_default_policy_versions(data: &mut IotData, policy: &str) {
    let prefix = format!("{policy}/");
    if let Some(m) = data.resources.get_mut("policy-versions") {
        for (k, rec) in m.iter_mut() {
            if k.starts_with(&prefix) {
                if let Some(o) = rec.as_object_mut() {
                    o.insert("isDefaultVersion".to_string(), Value::Bool(false));
                }
            }
        }
    }
}

/// CreatePolicy: persist the policy record and seed version 1 as the default,
/// so GetPolicy / GetPolicyVersion / ListPolicyVersions all round-trip.
fn create_policy(
    svc: &IotService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = labels.get("policyName").cloned().unwrap_or_default();
    let document = body_str(body, "policyDocument").unwrap_or("").to_string();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if data.get_resource("policies", &name).is_some() {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "ResourceAlreadyExistsException",
            format!("Policy cannot be created - name already exists (name={name})"),
        ));
    }
    let arn = mint_arn(ctx, "policies", &name);
    let policy_rec = json!({
        "policyName": name,
        "policyArn": arn,
        "policyDocument": document,
        "defaultVersionId": "1",
        "creationDate": super::now_epoch(),
        "lastModifiedDate": super::now_epoch(),
        "generationId": "1",
    });
    data.put_resource("policies", &name, policy_rec);
    store_policy_version(data, &name, &arn, &document, "1", true);
    Ok((
        ok_json(json!({
            "policyName": name,
            "policyArn": arn,
            "policyDocument": document,
            "policyVersionId": "1",
        })),
        true,
    ))
}

fn create_policy_version(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
    query: &[(String, String)],
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = labels.get("policyName").cloned().unwrap_or_default();
    let document = body_str(body, "policyDocument").unwrap_or("").to_string();
    let set_as_default = query_get(query, "setAsDefault") == Some("true");
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let Some(policy) = data.get_resource("policies", &name).cloned() else {
        return Err(super::engine::not_found(meta, &name));
    };
    let arn = policy
        .get("policyArn")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| mint_arn(ctx, "policies", &name));
    let vid = next_policy_version_id(data, &name);
    if set_as_default {
        clear_default_policy_versions(data, &name);
        if let Some(rec) = data
            .resources
            .get_mut("policies")
            .and_then(|m| m.get_mut(&name))
        {
            if let Some(o) = rec.as_object_mut() {
                o.insert("defaultVersionId".to_string(), Value::String(vid.clone()));
            }
        }
    }
    store_policy_version(data, &name, &arn, &document, &vid, set_as_default);
    Ok((
        ok_json(json!({
            "policyArn": arn,
            "policyDocument": document,
            "policyVersionId": vid,
            "isDefaultVersion": set_as_default,
        })),
        true,
    ))
}

fn get_policy_version(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = labels.get("policyName").cloned().unwrap_or_default();
    let vid = labels.get("policyVersionId").cloned().unwrap_or_default();
    let g = svc.state.read();
    match g
        .get(&ctx.account)
        .and_then(|d| d.get_resource("policy-versions", &policy_version_key(&name, &vid)))
    {
        Some(rec) => Ok((ok_json(super::build_output(meta, rec)), false)),
        None => Err(super::engine::not_found(meta, &vid)),
    }
}

fn list_policy_versions(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = labels.get("policyName").cloned().unwrap_or_default();
    let g = svc.state.read();
    let data = g.get(&ctx.account);
    if data
        .and_then(|d| d.get_resource("policies", &name))
        .is_none()
    {
        return Err(super::engine::not_found(meta, &name));
    }
    let prefix = format!("{name}/");
    let mut versions = Vec::new();
    if let Some(m) = data.and_then(|d| d.resources.get("policy-versions")) {
        for (k, rec) in m {
            if k.starts_with(&prefix) {
                versions.push(super::build_element(meta, rec));
            }
        }
    }
    Ok((ok_json(json!({ "policyVersions": versions })), false))
}

fn set_default_policy_version(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = labels.get("policyName").cloned().unwrap_or_default();
    let vid = labels.get("policyVersionId").cloned().unwrap_or_default();
    let key = policy_version_key(&name, &vid);
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if data.get_resource("policy-versions", &key).is_none() {
        return Err(super::engine::not_found(meta, &vid));
    }
    clear_default_policy_versions(data, &name);
    if let Some(rec) = data
        .resources
        .get_mut("policy-versions")
        .and_then(|m| m.get_mut(&key))
    {
        if let Some(o) = rec.as_object_mut() {
            o.insert("isDefaultVersion".to_string(), Value::Bool(true));
        }
    }
    if let Some(rec) = data
        .resources
        .get_mut("policies")
        .and_then(|m| m.get_mut(&name))
    {
        if let Some(o) = rec.as_object_mut() {
            o.insert("defaultVersionId".to_string(), Value::String(vid.clone()));
        }
    }
    Ok((ok_json(Value::Object(Map::new())), true))
}

fn delete_policy_version(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = labels.get("policyName").cloned().unwrap_or_default();
    let vid = labels.get("policyVersionId").cloned().unwrap_or_default();
    let key = policy_version_key(&name, &vid);
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let Some(rec) = data.get_resource("policy-versions", &key).cloned() else {
        return Err(super::engine::not_found(meta, &vid));
    };
    // AWS forbids deleting the default version (it must be deleted via
    // DeletePolicy); the operation declares DeleteConflictException for this.
    if rec.get("isDefaultVersion").and_then(Value::as_bool) == Some(true) {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "DeleteConflictException",
            "Cannot delete the default version of a policy.".to_string(),
        ));
    }
    data.remove_resource("policy-versions", &key);
    Ok((ok_json(Value::Object(Map::new())), true))
}

// ---------- provisioning templates + versions + claim ----------

fn provisioning_version_key(name: &str, vid: i64) -> String {
    format!("{name}/{vid}")
}

fn store_provisioning_version(
    data: &mut IotData,
    name: &str,
    vid: i64,
    template_body: &str,
    is_default: bool,
) {
    let rec = json!({
        "versionId": vid,
        "templateBody": template_body,
        "isDefaultVersion": is_default,
        "creationDate": super::now_epoch(),
    });
    data.put_resource(
        "provisioning-template-versions",
        &provisioning_version_key(name, vid),
        rec,
    );
}

fn next_provisioning_version_id(data: &IotData, name: &str) -> i64 {
    let prefix = format!("{name}/");
    let max = data
        .resources
        .get("provisioning-template-versions")
        .map(|m| {
            m.keys()
                .filter_map(|k| k.strip_prefix(&prefix))
                .filter_map(|s| s.parse::<i64>().ok())
                .max()
                .unwrap_or(0)
        })
        .unwrap_or(0);
    max + 1
}

fn clear_default_provisioning_versions(data: &mut IotData, name: &str) {
    let prefix = format!("{name}/");
    if let Some(m) = data.resources.get_mut("provisioning-template-versions") {
        for (k, rec) in m.iter_mut() {
            if k.starts_with(&prefix) {
                if let Some(o) = rec.as_object_mut() {
                    o.insert("isDefaultVersion".to_string(), Value::Bool(false));
                }
            }
        }
    }
}

fn create_provisioning_template(
    svc: &IotService,
    ctx: &Ctx,
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = body_str(body, "templateName").unwrap_or("").to_string();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    if data.get_resource("provisioning-templates", &name).is_some() {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "ResourceAlreadyExistsException",
            format!("Template with name {name} already exists."),
        ));
    }
    let arn = mint_arn(ctx, "provisioning-templates", &name);
    let template_body = body_str(body, "templateBody").unwrap_or("").to_string();
    let mut rec = Map::new();
    rec.insert("templateArn".to_string(), Value::String(arn.clone()));
    rec.insert("templateName".to_string(), Value::String(name.clone()));
    for key in [
        "description",
        "provisioningRoleArn",
        "type",
        "preProvisioningHook",
    ] {
        if let Some(v) = body.get(key) {
            rec.insert(key.to_string(), v.clone());
        }
    }
    rec.insert(
        "templateBody".to_string(),
        Value::String(template_body.clone()),
    );
    rec.insert(
        "enabled".to_string(),
        Value::Bool(body.get("enabled").and_then(Value::as_bool).unwrap_or(true)),
    );
    rec.insert("defaultVersionId".to_string(), Value::from(1));
    rec.insert("creationDate".to_string(), super::now_epoch());
    rec.insert("lastModifiedDate".to_string(), super::now_epoch());
    data.put_resource("provisioning-templates", &name, Value::Object(rec));
    store_provisioning_version(data, &name, 1, &template_body, true);
    Ok((
        ok_json(json!({
            "templateArn": arn,
            "templateName": name,
            "defaultVersionId": 1,
        })),
        true,
    ))
}

fn create_provisioning_template_version(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
    query: &[(String, String)],
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = labels.get("templateName").cloned().unwrap_or_default();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let Some(template) = data.get_resource("provisioning-templates", &name).cloned() else {
        return Err(super::engine::not_found(meta, &name));
    };
    let arn = template
        .get("templateArn")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| mint_arn(ctx, "provisioning-templates", &name));
    let template_body = body_str(body, "templateBody").unwrap_or("").to_string();
    let vid = next_provisioning_version_id(data, &name);
    let set_as_default = query_get(query, "setAsDefault") == Some("true");
    if set_as_default {
        clear_default_provisioning_versions(data, &name);
        if let Some(rec) = data
            .resources
            .get_mut("provisioning-templates")
            .and_then(|m| m.get_mut(&name))
        {
            if let Some(o) = rec.as_object_mut() {
                o.insert("defaultVersionId".to_string(), Value::from(vid));
            }
        }
    }
    store_provisioning_version(data, &name, vid, &template_body, set_as_default);
    Ok((
        ok_json(json!({
            "templateArn": arn,
            "templateName": name,
            "versionId": vid,
            "isDefaultVersion": set_as_default,
        })),
        true,
    ))
}

fn describe_provisioning_template_version(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = labels.get("templateName").cloned().unwrap_or_default();
    let vid = labels.get("versionId").cloned().unwrap_or_default();
    let g = svc.state.read();
    match g
        .get(&ctx.account)
        .and_then(|d| d.get_resource("provisioning-template-versions", &format!("{name}/{vid}")))
    {
        Some(rec) => Ok((ok_json(super::build_output(meta, rec)), false)),
        None => Err(super::engine::not_found(meta, &vid)),
    }
}

fn list_provisioning_template_versions(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = labels.get("templateName").cloned().unwrap_or_default();
    let g = svc.state.read();
    let data = g.get(&ctx.account);
    if data
        .and_then(|d| d.get_resource("provisioning-templates", &name))
        .is_none()
    {
        return Err(super::engine::not_found(meta, &name));
    }
    let prefix = format!("{name}/");
    let mut versions = Vec::new();
    if let Some(m) = data.and_then(|d| d.resources.get("provisioning-template-versions")) {
        for (k, rec) in m {
            if k.starts_with(&prefix) {
                versions.push(super::build_element(meta, rec));
            }
        }
    }
    Ok((ok_json(json!({ "versions": versions })), false))
}

fn create_provisioning_claim(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let name = labels.get("templateName").cloned().unwrap_or_default();
    {
        let g = svc.state.read();
        if g.get(&ctx.account)
            .and_then(|d| d.get_resource("provisioning-templates", &name))
            .is_none()
        {
            return Err(super::engine::not_found(meta, &name));
        }
    }
    let seq = { svc.state.write().get_or_create(&ctx.account).next_seq() };
    let cert_id = mint_hex64(&format!("{}:claim:{name}:{seq}", ctx.account));
    // The claim's certificate is a real, persisted (temporary) certificate.
    store_certificate(svc, ctx, "certificates", &cert_id, true);
    let expiration = {
        let millis = chrono::Utc::now().timestamp_millis() + 3_600_000;
        Value::from(millis as f64 / 1000.0)
    };
    Ok((
        ok_json(json!({
            "certificateId": cert_id,
            "certificatePem": placeholder_pem("CERTIFICATE", &cert_id),
            "keyPair": key_pair(&cert_id),
            "expiration": expiration,
        })),
        true,
    ))
}

// ---------- certificate transfer + thing registration ----------

fn transfer_certificate(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
    query: &[(String, String)],
    body: &Map<String, Value>,
) -> Result<(AwsResponse, bool), AwsServiceError> {
    let cert_id = labels.get("certificateId").cloned().unwrap_or_default();
    let target = query_get(query, "targetAwsAccount")
        .unwrap_or("")
        .to_string();
    let mut g = svc.state.write();
    let data = g.get_or_create(&ctx.account);
    let Some(mut rec) = data.get_resource("certificates", &cert_id).cloned() else {
        return Err(super::engine::not_found(meta, &cert_id));
    };
    let transferred_arn = format!("arn:aws:iot:{}:{}:cert/{}", ctx.region, target, cert_id);
    if let Some(o) = rec.as_object_mut() {
        o.insert(
            "status".to_string(),
            Value::String("PENDING_TRANSFER".to_string()),
        );
        o.insert("transferredTo".to_string(), Value::String(target));
        if let Some(m) = body.get("transferMessage") {
            o.insert("transferMessage".to_string(), m.clone());
        }
    }
    data.put_resource("certificates", &cert_id, rec);
    Ok((
        ok_json(json!({ "transferredCertificateArn": transferred_arn })),
        true,
    ))
}

/// RegisterThing (fleet provisioning): mint and persist a real certificate and
/// thing, returning the certificate PEM and the map of provisioned resource
/// ARNs (`resourceArns`).
fn register_thing(svc: &IotService, ctx: &Ctx) -> (AwsResponse, bool) {
    let seq = { svc.state.write().get_or_create(&ctx.account).next_seq() };
    let thing_name = format!("provisioned-thing-{seq}");
    let cert_id = mint_hex64(&format!("{}:register:{seq}", ctx.account));
    let cert_arn = store_certificate(svc, ctx, "certificates", &cert_id, true);
    let thing_arn = mint_arn(ctx, "things", &thing_name);
    let thing_id = super::mint_uuid(&format!("{}:things:{thing_name}", ctx.account));
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        data.put_resource(
            "things",
            &thing_name,
            json!({
                "thingName": thing_name,
                "thingArn": thing_arn.clone(),
                "thingId": thing_id,
            }),
        );
    }
    (
        ok_json(json!({
            "certificatePem": placeholder_pem("CERTIFICATE", &cert_id),
            "resourceArns": {
                "thing": thing_arn,
                "certificate": cert_arn,
            },
        })),
        true,
    )
}

// ---------- long-running tasks ----------

/// Start a long-running task. The taskId is either the client-supplied URI
/// label or, when the operation mints it, a deterministic id; a task record is
/// persisted under the operation's resource type so the matching Describe reads
/// back.
fn start_task(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    labels: &HashMap<String, String>,
    body: &Map<String, Value>,
) -> (AwsResponse, bool) {
    let rtype = super::resource_type(meta);
    let task_id = match labels.get("taskId") {
        Some(id) => id.clone(),
        None => {
            let seq = { svc.state.write().get_or_create(&ctx.account).next_seq() };
            super::mint_uuid(&format!("{}:{rtype}:{seq}", ctx.account))
        }
    };
    let mut rec = body.clone();
    rec.insert("taskId".to_string(), Value::String(task_id.clone()));
    rec.entry("status")
        .or_insert_with(|| Value::String("IN_PROGRESS".to_string()));
    rec.entry("taskStatus")
        .or_insert_with(|| Value::String("IN_PROGRESS".to_string()));
    rec.insert("creationDate".to_string(), super::now_epoch());
    {
        let mut g = svc.state.write();
        let data = g.get_or_create(&ctx.account);
        data.put_resource(&rtype, &task_id, Value::Object(rec));
    }
    (ok_json(json!({ "taskId": task_id })), true)
}

// ---------- principal/thing/group inversions ----------

/// The suffixes of every `prefix<a>` relation key whose value set contains
/// `target` — i.e. the inverse of the stored edges.
fn inverse_relation(data: Option<&IotData>, prefix: &str, target: &str) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(d) = data {
        for (key, values) in &d.relations {
            if let Some(a) = key.strip_prefix(prefix) {
                if values.iter().any(|v| v == target) {
                    out.push(a.to_string());
                }
            }
        }
    }
    out
}

fn list_principal_things(
    svc: &IotService,
    ctx: &Ctx,
    meta: &OpMeta,
    headers: &HeaderMap,
    v2: bool,
) -> (AwsResponse, bool) {
    let principal = header_principal(meta, headers).unwrap_or("").to_string();
    let g = svc.state.read();
    let things = inverse_relation(g.get(&ctx.account), "thing-principals:", &principal);
    if v2 {
        let objs: Vec<Value> = things
            .into_iter()
            .map(|t| json!({ "thingName": t }))
            .collect();
        (ok_json(json!({ "principalThingObjects": objs })), false)
    } else {
        (ok_json(json!({ "things": things })), false)
    }
}

fn list_thing_groups_for_thing(
    svc: &IotService,
    ctx: &Ctx,
    labels: &HashMap<String, String>,
) -> (AwsResponse, bool) {
    let thing = lbl(labels, "thingName").unwrap_or("").to_string();
    let g = svc.state.read();
    let groups = inverse_relation(g.get(&ctx.account), "group-things:", &thing);
    let objs: Vec<Value> = groups
        .into_iter()
        .map(|grp| {
            let arn = mint_arn(ctx, "thing-groups", &grp);
            json!({ "groupName": grp, "groupArn": arn })
        })
        .collect();
    (ok_json(json!({ "thingGroups": objs })), false)
}
