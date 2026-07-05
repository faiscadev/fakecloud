//! AWS Config data-plane logic: cross-service configuration recording, managed
//! rule evaluation, and the `SelectResourceConfig` query subset.
//!
//! Recording reaches into the live state of other FakeCloud services (S3, EC2,
//! IAM) and synthesizes genuine `ConfigurationItem`s from what actually exists,
//! rather than fabricating placeholder resources. Rule evaluation runs real
//! checks against those recorded items and produces true COMPLIANT /
//! NON_COMPLIANT results.

use chrono::Utc;
use serde_json::{json, Value};

use fakecloud_ec2::SharedEc2State;
use fakecloud_iam::SharedIamState;
use fakecloud_s3::SharedS3State;

use crate::state::{resource_key, AccountState, ConfigurationItem, EvaluationResult};

/// Handles to the other services' state that AWS Config records from. All
/// optional so the service degrades gracefully in a minimal deployment (and in
/// unit tests) — a resource type whose backing service is absent simply
/// records nothing rather than panicking.
#[derive(Clone, Default)]
pub struct CrossServiceStates {
    pub s3: Option<SharedS3State>,
    pub iam: Option<SharedIamState>,
    pub ec2: Option<SharedEc2State>,
}

/// AWS resource types Config records natively in this implementation.
pub const SUPPORTED_RESOURCE_TYPES: &[&str] = &[
    "AWS::S3::Bucket",
    "AWS::EC2::Instance",
    "AWS::EC2::SecurityGroup",
    "AWS::EC2::VPC",
    "AWS::IAM::User",
    "AWS::IAM::Role",
    "AWS::IAM::Policy",
];

/// A single discovered resource, before it is turned into a full
/// `ConfigurationItem`.
pub struct DiscoveredResource {
    pub resource_type: String,
    pub resource_id: String,
    pub resource_name: Option<String>,
    pub region: String,
    pub availability_zone: String,
    pub arn: String,
    pub tags: std::collections::BTreeMap<String, String>,
    /// The resource configuration as a JSON value.
    pub configuration: Value,
}

/// Discover every supported resource that currently exists across the wired
/// services for `account_id`.
pub fn discover_all(
    states: &CrossServiceStates,
    account_id: &str,
    region: &str,
) -> Vec<DiscoveredResource> {
    let mut out = Vec::new();
    discover_s3(states, account_id, region, &mut out);
    discover_ec2(states, account_id, region, &mut out);
    discover_iam(states, account_id, region, &mut out);
    out
}

fn discover_s3(
    states: &CrossServiceStates,
    account_id: &str,
    region: &str,
    out: &mut Vec<DiscoveredResource>,
) {
    let Some(s3) = &states.s3 else { return };
    let guard = s3.read();
    let Some(st) = guard.get(account_id) else {
        return;
    };
    for (name, bucket) in &st.buckets {
        let tags = bucket.tags.clone();
        let config = json!({
            "name": name,
            "creationDate": bucket.creation_date.to_rfc3339(),
            "bucketVersioningConfiguration": {
                "status": bucket.versioning.clone().unwrap_or_else(|| "Off".to_string()),
                "isMfaDeleteEnabled": bucket.mfa_delete.as_deref() == Some("Enabled"),
            },
            "serverSideEncryptionConfiguration": bucket.encryption_config,
            "publicAccessBlockConfiguration": bucket.public_access_block,
            "bucketPolicy": bucket.policy,
            "region": bucket.region,
        });
        out.push(DiscoveredResource {
            resource_type: "AWS::S3::Bucket".into(),
            resource_id: name.clone(),
            resource_name: Some(name.clone()),
            region: region.to_string(),
            availability_zone: "Regional".into(),
            arn: format!("arn:aws:s3:::{name}"),
            tags,
            configuration: config,
        });
    }
}

fn discover_ec2(
    states: &CrossServiceStates,
    account_id: &str,
    region: &str,
    out: &mut Vec<DiscoveredResource>,
) {
    let Some(ec2) = &states.ec2 else { return };
    let guard = ec2.read();
    let Some(st) = guard.get(account_id) else {
        return;
    };
    let tags_for = |id: &str| -> std::collections::BTreeMap<String, String> {
        st.tags
            .get(id)
            .map(|v| v.iter().map(|t| (t.key.clone(), t.value.clone())).collect())
            .unwrap_or_default()
    };
    for (id, inst) in &st.instances {
        if inst.state_name == "terminated" {
            continue;
        }
        let config = json!({
            "instanceId": id,
            "imageId": inst.image_id,
            "instanceType": inst.instance_type,
            "state": { "name": inst.state_name, "code": inst.state_code },
            "privateIpAddress": inst.private_ip,
            "publicIpAddress": inst.public_ip,
            "subnetId": inst.subnet_id,
            "vpcId": inst.vpc_id,
            "securityGroups": inst.security_group_ids,
            "keyName": inst.key_name,
        });
        let name = tags_for(id).get("Name").cloned();
        out.push(DiscoveredResource {
            resource_type: "AWS::EC2::Instance".into(),
            resource_id: id.clone(),
            resource_name: name,
            region: region.to_string(),
            availability_zone: inst.az.clone(),
            arn: format!("arn:aws:ec2:{region}:{account_id}:instance/{id}"),
            tags: tags_for(id),
            configuration: config,
        });
    }
    for (id, sg) in &st.security_groups {
        let ingress: Vec<Value> = sg
            .rules
            .iter()
            .filter(|r| !r.is_egress)
            .map(security_group_rule_json)
            .collect();
        let egress: Vec<Value> = sg
            .rules
            .iter()
            .filter(|r| r.is_egress)
            .map(security_group_rule_json)
            .collect();
        let config = json!({
            "groupId": sg.group_id,
            "groupName": sg.group_name,
            "description": sg.description,
            "vpcId": sg.vpc_id,
            "ipPermissions": ingress,
            "ipPermissionsEgress": egress,
        });
        out.push(DiscoveredResource {
            resource_type: "AWS::EC2::SecurityGroup".into(),
            resource_id: id.clone(),
            resource_name: Some(sg.group_name.clone()),
            region: region.to_string(),
            availability_zone: "Regional".into(),
            arn: format!("arn:aws:ec2:{region}:{account_id}:security-group/{id}"),
            tags: tags_for(id),
            configuration: config,
        });
    }
    for (id, vpc) in &st.vpcs {
        let config = json!({
            "vpcId": id,
            "cidrBlock": vpc.cidr_block,
            "state": vpc.state,
            "isDefault": vpc.is_default,
        });
        out.push(DiscoveredResource {
            resource_type: "AWS::EC2::VPC".into(),
            resource_id: id.clone(),
            resource_name: tags_for(id).get("Name").cloned(),
            region: region.to_string(),
            availability_zone: "Regional".into(),
            arn: format!("arn:aws:ec2:{region}:{account_id}:vpc/{id}"),
            tags: tags_for(id),
            configuration: config,
        });
    }
}

fn security_group_rule_json(r: &fakecloud_ec2::state::SecurityGroupRule) -> Value {
    let mut ranges = Vec::new();
    if let Some(c) = &r.cidr_ipv4 {
        ranges.push(json!({ "cidrIp": c }));
    }
    json!({
        "ipProtocol": r.ip_protocol,
        "fromPort": r.from_port,
        "toPort": r.to_port,
        "ipRanges": ranges.iter().filter_map(|v| v.get("cidrIp").and_then(Value::as_str)).collect::<Vec<_>>(),
        "ipv4Ranges": ranges,
    })
}

fn discover_iam(
    states: &CrossServiceStates,
    account_id: &str,
    _region: &str,
    out: &mut Vec<DiscoveredResource>,
) {
    let Some(iam) = &states.iam else { return };
    let guard = iam.read();
    let Some(st) = guard.get(account_id) else {
        return;
    };
    for (name, user) in &st.users {
        let attached = st.user_policies.get(name).cloned().unwrap_or_default();
        let inline: Vec<String> = st
            .user_inline_policies
            .get(name)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        let config = json!({
            "userName": name,
            "userId": user.user_id,
            "arn": user.arn,
            "path": user.path,
            "attachedManagedPolicies": attached,
            "userPolicyList": inline,
        });
        out.push(DiscoveredResource {
            resource_type: "AWS::IAM::User".into(),
            resource_id: user.user_id.clone(),
            resource_name: Some(name.clone()),
            region: "global".into(),
            availability_zone: "Not Applicable".into(),
            arn: user.arn.clone(),
            tags: user
                .tags
                .iter()
                .map(|t| (t.key.clone(), t.value.clone()))
                .collect(),
            configuration: config,
        });
    }
    for (name, role) in &st.roles {
        let attached = st.role_policies.get(name).cloned().unwrap_or_default();
        let config = json!({
            "roleName": name,
            "roleId": role.role_id,
            "arn": role.arn,
            "path": role.path,
            "assumeRolePolicyDocument": role.assume_role_policy_document,
            "attachedManagedPolicies": attached,
        });
        out.push(DiscoveredResource {
            resource_type: "AWS::IAM::Role".into(),
            resource_id: role.role_id.clone(),
            resource_name: Some(name.clone()),
            region: "global".into(),
            availability_zone: "Not Applicable".into(),
            arn: role.arn.clone(),
            tags: role
                .tags
                .iter()
                .map(|t| (t.key.clone(), t.value.clone()))
                .collect(),
            configuration: config,
        });
    }
    for (arn, policy) in &st.policies {
        let config = json!({
            "policyName": policy.policy_name,
            "policyId": policy.policy_id,
            "arn": arn,
            "path": policy.path,
            "defaultVersionId": policy.default_version_id,
            "attachmentCount": policy.attachment_count,
        });
        out.push(DiscoveredResource {
            resource_type: "AWS::IAM::Policy".into(),
            resource_id: policy.policy_id.clone(),
            resource_name: Some(policy.policy_name.clone()),
            region: "global".into(),
            availability_zone: "Not Applicable".into(),
            arn: arn.clone(),
            tags: policy
                .tags
                .iter()
                .map(|t| (t.key.clone(), t.value.clone()))
                .collect(),
            configuration: config,
        });
    }
}

/// Fold newly-discovered cross-service resources into the account's recorded
/// configuration-item history. A new item is appended only when a resource is
/// new or its configuration changed since the last recorded item, so history
/// grows exactly like real Config (one item per configuration state).
pub fn sync_recorded_items(
    account: &mut AccountState,
    states: &CrossServiceStates,
    account_id: &str,
    region: &str,
) {
    let discovered = discover_all(states, account_id, region);
    let now = Utc::now();
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for res in discovered {
        let key = resource_key(&res.resource_type, &res.resource_id);
        seen.insert(key.clone());
        let config_str = serde_json::to_string(&res.configuration).unwrap_or_else(|_| "{}".into());
        let history = account.config_items.entry(key).or_default();
        let changed = history
            .last()
            .map(|last| {
                last.configuration != config_str
                    || last.configuration_item_status == "ResourceDeleted"
            })
            .unwrap_or(true);
        if changed {
            let state_id = (history.len() as u64 + 1).to_string();
            history.push(ConfigurationItem {
                version: "1.3".into(),
                account_id: account_id.to_string(),
                configuration_item_capture_time: now,
                configuration_item_status: if history.is_empty() {
                    "ResourceDiscovered".into()
                } else {
                    "OK".into()
                },
                configuration_state_id: state_id,
                arn: res.arn,
                resource_type: res.resource_type,
                resource_id: res.resource_id,
                resource_name: res.resource_name,
                aws_region: res.region,
                availability_zone: res.availability_zone,
                resource_creation_time: Some(now),
                tags: res.tags,
                configuration: config_str,
                supplementary_configuration: Default::default(),
            });
        }
    }
    // Mark resources that vanished from the live services as deleted (only for
    // the cross-service types; externally PutResourceConfig'd items are left
    // untouched).
    for (key, history) in account.config_items.iter_mut() {
        let Some((rtype, _)) = key.split_once('\u{1}') else {
            continue;
        };
        if !SUPPORTED_RESOURCE_TYPES.contains(&rtype) {
            continue;
        }
        if seen.contains(key) {
            continue;
        }
        let already_deleted = history
            .last()
            .map(|l| l.configuration_item_status.starts_with("ResourceDeleted"))
            .unwrap_or(true);
        if !already_deleted {
            if let Some(last) = history.last().cloned() {
                let state_id = (history.len() as u64 + 1).to_string();
                history.push(ConfigurationItem {
                    configuration_item_capture_time: now,
                    configuration_item_status: "ResourceDeleted".into(),
                    configuration_state_id: state_id,
                    ..last
                });
            }
        }
    }
}

// ─── Managed rule evaluation ─────────────────────────────────────────────

/// The compliance verdict Config reports for a resource under a rule.
pub struct RuleOutcome {
    pub resource_type: String,
    pub resource_id: String,
    pub compliance_type: String,
    pub annotation: Option<String>,
}

/// Evaluate one config rule against the recorded configuration items. Returns
/// per-resource outcomes for the resource types the rule applies to. For AWS
/// managed rules that are not implemented here, returns a single
/// `INSUFFICIENT_DATA` outcome rather than falsely reporting COMPLIANT.
pub fn evaluate_managed_rule(
    source_identifier: &str,
    input_parameters: &Value,
    account: &AccountState,
) -> Vec<RuleOutcome> {
    // Latest recorded item per resource (skip deleted).
    let latest: Vec<&ConfigurationItem> = account
        .config_items
        .values()
        .filter_map(|h| h.last())
        .filter(|ci| !ci.configuration_item_status.starts_with("ResourceDeleted"))
        .collect();

    let by_type = |t: &str| -> Vec<&&ConfigurationItem> {
        latest.iter().filter(|ci| ci.resource_type == t).collect()
    };
    let cfg = |ci: &ConfigurationItem| -> Value {
        serde_json::from_str(&ci.configuration).unwrap_or(Value::Null)
    };

    let mut out = Vec::new();
    match source_identifier {
        "S3_BUCKET_VERSIONING_ENABLED" => {
            for ci in by_type("AWS::S3::Bucket") {
                let status = cfg(ci)
                    .pointer("/bucketVersioningConfiguration/status")
                    .and_then(Value::as_str)
                    .unwrap_or("Off")
                    .to_string();
                let compliant = status == "Enabled";
                out.push(outcome(
                    ci,
                    compliant,
                    if compliant {
                        None
                    } else {
                        Some("Bucket versioning is not enabled".into())
                    },
                ));
            }
        }
        "S3_BUCKET_SERVER_SIDE_ENCRYPTION_ENABLED" => {
            for ci in by_type("AWS::S3::Bucket") {
                let enc = cfg(ci)
                    .get("serverSideEncryptionConfiguration")
                    .map(|v| !v.is_null())
                    .unwrap_or(false);
                out.push(outcome(
                    ci,
                    enc,
                    if enc {
                        None
                    } else {
                        Some("Default server-side encryption is not configured".into())
                    },
                ));
            }
        }
        "S3_BUCKET_PUBLIC_READ_PROHIBITED" | "S3_BUCKET_PUBLIC_WRITE_PROHIBITED" => {
            for ci in by_type("AWS::S3::Bucket") {
                // COMPLIANT when a public access block exists (blocking public
                // access) and there is no wildcard bucket policy.
                let pab = cfg(ci)
                    .get("publicAccessBlockConfiguration")
                    .map(|v| !v.is_null())
                    .unwrap_or(false);
                let policy_public = cfg(ci)
                    .get("bucketPolicy")
                    .and_then(Value::as_str)
                    .map(|p| p.contains("\"Principal\":\"*\"") || p.contains("\"AWS\":\"*\""))
                    .unwrap_or(false);
                let compliant = pab && !policy_public;
                out.push(outcome(
                    ci,
                    compliant,
                    if compliant {
                        None
                    } else {
                        Some("Bucket may allow public access".into())
                    },
                ));
            }
        }
        "IAM_USER_NO_POLICIES_CHECK" => {
            for ci in by_type("AWS::IAM::User") {
                let c = cfg(ci);
                let attached = c
                    .get("attachedManagedPolicies")
                    .and_then(Value::as_array)
                    .map(|a| a.is_empty())
                    .unwrap_or(true);
                let inline = c
                    .get("userPolicyList")
                    .and_then(Value::as_array)
                    .map(|a| a.is_empty())
                    .unwrap_or(true);
                let compliant = attached && inline;
                out.push(outcome(
                    ci,
                    compliant,
                    if compliant {
                        None
                    } else {
                        Some("IAM policy attached directly to user".into())
                    },
                ));
            }
        }
        "EC2_INSTANCE_NO_PUBLIC_IP" => {
            for ci in by_type("AWS::EC2::Instance") {
                let has_public = cfg(ci)
                    .get("publicIpAddress")
                    .map(|v| !v.is_null())
                    .unwrap_or(false);
                out.push(outcome(
                    ci,
                    !has_public,
                    if has_public {
                        Some("Instance has a public IP address".into())
                    } else {
                        None
                    },
                ));
            }
        }
        "VPC_DEFAULT_SECURITY_GROUP_CLOSED" => {
            for ci in by_type("AWS::EC2::SecurityGroup") {
                let c = cfg(ci);
                if c.get("groupName").and_then(Value::as_str) != Some("default") {
                    continue;
                }
                let ingress_empty = c
                    .get("ipPermissions")
                    .and_then(Value::as_array)
                    .map(|a| a.is_empty())
                    .unwrap_or(true);
                let egress_empty = c
                    .get("ipPermissionsEgress")
                    .and_then(Value::as_array)
                    .map(|a| a.is_empty())
                    .unwrap_or(true);
                let compliant = ingress_empty && egress_empty;
                out.push(outcome(
                    ci,
                    compliant,
                    if compliant {
                        None
                    } else {
                        Some("Default security group is not closed".into())
                    },
                ));
            }
        }
        "INCOMING_SSH_DISABLED" | "RESTRICTED_INCOMING_TRAFFIC" => {
            let blocked_port: i64 = if source_identifier == "INCOMING_SSH_DISABLED" {
                22
            } else {
                input_parameters
                    .get("blockedPort1")
                    .and_then(param_as_i64)
                    .unwrap_or(22)
            };
            for ci in by_type("AWS::EC2::SecurityGroup") {
                let c = cfg(ci);
                let open = c
                    .get("ipPermissions")
                    .and_then(Value::as_array)
                    .map(|rules| rules.iter().any(|r| rule_opens_port(r, blocked_port)))
                    .unwrap_or(false);
                out.push(outcome(
                    ci,
                    !open,
                    if open {
                        Some(format!(
                            "Security group allows unrestricted access to port {blocked_port}"
                        ))
                    } else {
                        None
                    },
                ));
            }
        }
        "REQUIRED_TAGS" => {
            let required: Vec<String> = (1..=6)
                .filter_map(|i| {
                    input_parameters
                        .get(format!("tag{i}Key"))
                        .and_then(param_as_str)
                })
                .collect();
            for ci in &latest {
                if required.is_empty() {
                    out.push(outcome(ci, true, None));
                    continue;
                }
                let missing: Vec<&String> = required
                    .iter()
                    .filter(|k| !ci.tags.contains_key(*k))
                    .collect();
                let compliant = missing.is_empty();
                out.push(outcome(
                    ci,
                    compliant,
                    if compliant {
                        None
                    } else {
                        Some(format!("Missing required tags: {missing:?}"))
                    },
                ));
            }
        }
        _ => {
            // Not implemented: report INSUFFICIENT_DATA honestly rather than
            // faking a COMPLIANT verdict.
            out.push(RuleOutcome {
                resource_type: String::new(),
                resource_id: String::new(),
                compliance_type: "INSUFFICIENT_DATA".into(),
                annotation: Some(format!(
                    "Managed rule {source_identifier} is not evaluated by this implementation"
                )),
            });
        }
    }
    out
}

fn outcome(ci: &ConfigurationItem, compliant: bool, annotation: Option<String>) -> RuleOutcome {
    RuleOutcome {
        resource_type: ci.resource_type.clone(),
        resource_id: ci.resource_id.clone(),
        compliance_type: if compliant {
            "COMPLIANT".into()
        } else {
            "NON_COMPLIANT".into()
        },
        annotation,
    }
}

fn rule_opens_port(rule: &Value, port: i64) -> bool {
    let from = rule.get("fromPort").and_then(Value::as_i64).unwrap_or(0);
    let to = rule.get("toPort").and_then(Value::as_i64).unwrap_or(65535);
    let covers = from <= port && port <= to;
    let public = rule
        .get("ipRanges")
        .and_then(Value::as_array)
        .map(|a| a.iter().any(|c| c.as_str() == Some("0.0.0.0/0")))
        .unwrap_or(false);
    covers && public
}

fn param_as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn param_as_str(v: &Value) -> Option<String> {
    v.as_str().map(|s| s.to_string())
}

/// Turn a [`RuleOutcome`] into a persisted [`EvaluationResult`].
pub fn outcome_to_result(rule_name: &str, o: &RuleOutcome) -> EvaluationResult {
    let now = Utc::now();
    EvaluationResult {
        resource_type: o.resource_type.clone(),
        resource_id: o.resource_id.clone(),
        rule_name: rule_name.to_string(),
        compliance_type: o.compliance_type.clone(),
        annotation: o.annotation.clone(),
        result_recorded_time: now,
        config_rule_invoked_time: now,
        ordering_timestamp: now,
    }
}

// ─── SelectResourceConfig query subset ───────────────────────────────────

/// Evaluate a Config advanced-query `SELECT` expression against recorded items.
///
/// Supported grammar (a real, documented subset of the Config query language):
/// `SELECT <field>[, <field>...] [WHERE <cond> [AND <cond>...]]`, where each
/// `<cond>` is `<field> = '<value>'` or `<field> IN ('a', 'b')`. Fields are
/// dotted paths resolved against the item: bare `resourceId`, `resourceType`,
/// `resourceName`, `arn`, `awsRegion`, `tags`, or `configuration.<path>`.
/// Returns `(rows, error)`; on a parse error `rows` is empty and `error` is set.
pub fn run_select(expression: &str, account: &AccountState) -> Result<Vec<Value>, String> {
    let expr = expression.trim().trim_end_matches(';');
    let lower = expr.to_ascii_lowercase();
    if !lower.starts_with("select ") {
        return Err("Query must start with SELECT".into());
    }
    let after_select = &expr[7..];
    let (fields_part, where_part) = match after_select.to_ascii_lowercase().find(" where ") {
        Some(idx) => (&after_select[..idx], Some(after_select[idx + 7..].trim())),
        None => (after_select, None),
    };
    let fields: Vec<String> = fields_part
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if fields.is_empty() {
        return Err("SELECT requires at least one field".into());
    }
    let conditions = match where_part {
        Some(w) => parse_conditions(w)?,
        None => Vec::new(),
    };

    let latest: Vec<&ConfigurationItem> = account
        .config_items
        .values()
        .filter_map(|h| h.last())
        .filter(|ci| !ci.configuration_item_status.starts_with("ResourceDeleted"))
        .collect();

    let mut rows = Vec::new();
    for ci in latest {
        if !conditions.iter().all(|c| c.matches(ci)) {
            continue;
        }
        let mut row = serde_json::Map::new();
        for f in &fields {
            if f == "*" {
                row.insert("resourceId".into(), json!(ci.resource_id));
                row.insert("resourceType".into(), json!(ci.resource_type));
                continue;
            }
            if let Some(v) = field_value(ci, f) {
                insert_dotted(&mut row, f, v);
            }
        }
        rows.push(Value::Object(row));
    }
    Ok(rows)
}

struct Condition {
    field: String,
    values: Vec<String>,
}

impl Condition {
    fn matches(&self, ci: &ConfigurationItem) -> bool {
        match field_value(ci, &self.field) {
            Some(Value::String(s)) => self.values.iter().any(|v| v == &s),
            Some(other) => self.values.iter().any(|v| v == &other.to_string()),
            None => false,
        }
    }
}

fn parse_conditions(w: &str) -> Result<Vec<Condition>, String> {
    let mut conds = Vec::new();
    for part in split_and(w) {
        let part = part.trim();
        if let Some((f, rest)) = part.split_once(" IN ").or_else(|| part.split_once(" in ")) {
            let list = rest.trim().trim_start_matches('(').trim_end_matches(')');
            let values: Vec<String> = list.split(',').map(|s| unquote(s.trim())).collect();
            conds.push(Condition {
                field: f.trim().to_string(),
                values,
            });
        } else if let Some((f, v)) = part.split_once('=') {
            conds.push(Condition {
                field: f.trim().to_string(),
                values: vec![unquote(v.trim())],
            });
        } else {
            return Err(format!("Unsupported condition: {part}"));
        }
    }
    Ok(conds)
}

fn split_and(w: &str) -> Vec<String> {
    // Split on " AND " outside of quotes.
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    let bytes: Vec<char> = w.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == '\'' {
            in_quote = !in_quote;
            cur.push(c);
            i += 1;
            continue;
        }
        if !in_quote {
            let rest: String = bytes[i..].iter().collect();
            let low = rest.to_ascii_lowercase();
            if low.starts_with(" and ") {
                parts.push(cur.trim().to_string());
                cur.clear();
                i += 5;
                continue;
            }
        }
        cur.push(c);
        i += 1;
    }
    if !cur.trim().is_empty() {
        parts.push(cur.trim().to_string());
    }
    parts
}

fn unquote(s: &str) -> String {
    s.trim().trim_matches('\'').trim_matches('"').to_string()
}

fn field_value(ci: &ConfigurationItem, field: &str) -> Option<Value> {
    match field {
        "resourceId" => Some(json!(ci.resource_id)),
        "resourceType" => Some(json!(ci.resource_type)),
        "resourceName" => ci.resource_name.clone().map(|v| json!(v)),
        "arn" => Some(json!(ci.arn)),
        "awsRegion" => Some(json!(ci.aws_region)),
        "availabilityZone" => Some(json!(ci.availability_zone)),
        "accountId" => Some(json!(ci.account_id)),
        _ => {
            if let Some(path) = field.strip_prefix("configuration.") {
                let cfg: Value = serde_json::from_str(&ci.configuration).ok()?;
                let pointer = format!("/{}", path.replace('.', "/"));
                cfg.pointer(&pointer).cloned()
            } else if let Some(tag) = field.strip_prefix("tags.") {
                ci.tags.get(tag).map(|v| json!(v))
            } else {
                None
            }
        }
    }
}

fn insert_dotted(row: &mut serde_json::Map<String, Value>, field: &str, value: Value) {
    // Config returns dotted select fields as the leaf name.
    let leaf = field.rsplit('.').next().unwrap_or(field);
    row.insert(leaf.to_string(), value);
}
