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

/// Whether an S3 bucket with `bucket_name` exists in the wired S3 service.
/// Returns `None` when S3 is not wired (validation is then skipped so the
/// service degrades gracefully in minimal deployments and unit tests). S3
/// bucket names are globally unique, so every account is searched.
pub fn s3_bucket_exists(states: &CrossServiceStates, bucket_name: &str) -> Option<bool> {
    let s3 = states.s3.as_ref()?;
    let guard = s3.read();
    let exists = guard
        .iter()
        .any(|(_, st)| st.buckets.contains_key(bucket_name));
    Some(exists)
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
        // A grant is public when it targets the AllUsers / AuthenticatedUsers
        // groups, or when a public canned ACL is set.
        let public_read = bucket.acl.as_deref().is_some_and(|a| {
            matches!(
                a,
                "public-read" | "public-read-write" | "authenticated-read"
            )
        }) || bucket.acl_grants.iter().any(|g| {
            is_public_grantee_uri(g.grantee_uri.as_deref())
                && matches!(g.permission.as_str(), "READ" | "FULL_CONTROL")
        });
        let public_write = bucket.acl.as_deref() == Some("public-read-write")
            || bucket.acl_grants.iter().any(|g| {
                is_public_grantee_uri(g.grantee_uri.as_deref())
                    && matches!(g.permission.as_str(), "WRITE" | "FULL_CONTROL")
            });
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
            "grantsPublicRead": public_read,
            "grantsPublicWrite": public_write,
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

/// The `configuration` string of a discovered resource, keyed by its stable
/// `resource_key`. Computed once so the read-only [`sync_would_change`] check
/// and the mutating [`apply_recorded_items`] agree exactly.
fn discovered_config(res: &DiscoveredResource) -> String {
    serde_json::to_string(&res.configuration).unwrap_or_else(|_| "{}".into())
}

/// Read-only check: would folding `discovered` into `account`'s recorded
/// history change anything? Lets the caller skip taking a write lock (and the
/// full re-record) on a pure read when nothing has changed since last sync.
pub fn sync_would_change(account: &AccountState, discovered: &[DiscoveredResource]) -> bool {
    let mut seen = std::collections::BTreeSet::new();
    for res in discovered {
        let key = resource_key(&res.resource_type, &res.resource_id);
        seen.insert(key.clone());
        let changed = account
            .config_items
            .get(&key)
            .and_then(|h| h.last())
            .map(|last| {
                last.configuration != discovered_config(res)
                    || last.configuration_item_status == "ResourceDeleted"
            })
            .unwrap_or(true);
        if changed {
            return true;
        }
    }
    // A natively-recorded resource that vanished needs a delete marker.
    for (key, history) in &account.config_items {
        let Some((rtype, _)) = key.split_once('\u{1}') else {
            continue;
        };
        if !SUPPORTED_RESOURCE_TYPES.contains(&rtype) || seen.contains(key) {
            continue;
        }
        match history.last() {
            Some(last)
                if !last.externally_recorded
                    && !last
                        .configuration_item_status
                        .starts_with("ResourceDeleted") =>
            {
                return true;
            }
            _ => {}
        }
    }
    false
}

/// Fold discovered cross-service resources into the account's recorded
/// configuration-item history. A new item is appended only when a resource is
/// new or its configuration changed since the last recorded item, so history
/// grows exactly like real Config (one item per configuration state).
/// Externally recorded items (`PutResourceConfig`) are never delete-marked here.
pub fn apply_recorded_items(
    account: &mut AccountState,
    discovered: Vec<DiscoveredResource>,
    account_id: &str,
) {
    let now = Utc::now();
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for res in discovered {
        let key = resource_key(&res.resource_type, &res.resource_id);
        seen.insert(key.clone());
        let config_str = discovered_config(&res);
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
                externally_recorded: false,
            });
        }
    }
    // Mark natively-recorded resources that vanished from the live services as
    // deleted. Externally recorded items (`PutResourceConfig`) are owned by the
    // caller and left untouched — never clobbered with a spurious delete.
    for (key, history) in account.config_items.iter_mut() {
        let Some((rtype, _)) = key.split_once('\u{1}') else {
            continue;
        };
        if !SUPPORTED_RESOURCE_TYPES.contains(&rtype) || seen.contains(key) {
            continue;
        }
        let Some(last) = history.last() else { continue };
        if last.externally_recorded
            || last
                .configuration_item_status
                .starts_with("ResourceDeleted")
        {
            continue;
        }
        let last = last.clone();
        let state_id = (history.len() as u64 + 1).to_string();
        history.push(ConfigurationItem {
            configuration_item_capture_time: now,
            configuration_item_status: "ResourceDeleted".into(),
            configuration_state_id: state_id,
            ..last
        });
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
    scope: &Value,
    account: &AccountState,
) -> Vec<RuleOutcome> {
    // Latest recorded item per resource (skip deleted), restricted to the
    // rule's Scope (ComplianceResourceTypes / ComplianceResourceId / TagKey)
    // so a rule never evaluates resources outside its declared scope.
    let latest: Vec<&ConfigurationItem> = account
        .config_items
        .values()
        .filter_map(|h| h.last())
        .filter(|ci| !ci.configuration_item_status.starts_with("ResourceDeleted"))
        .filter(|ci| item_in_scope(ci, scope))
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
            let want_write = source_identifier == "S3_BUCKET_PUBLIC_WRITE_PROHIBITED";
            for ci in by_type("AWS::S3::Bucket") {
                let c = cfg(ci);
                // A Public Access Block, when present, blocks public access, so
                // the bucket is COMPLIANT regardless of grants/policy.
                let pab = c
                    .get("publicAccessBlockConfiguration")
                    .map(|v| !v.is_null())
                    .unwrap_or(false);
                // Otherwise the bucket is NON_COMPLIANT only if it ACTUALLY
                // grants public access via a public ACL grant or a bucket policy
                // that allows the `*` principal. A normal private bucket with no
                // Public Access Block is COMPLIANT (matches AWS).
                let public_acl = c
                    .get(if want_write {
                        "grantsPublicWrite"
                    } else {
                        "grantsPublicRead"
                    })
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let policy_public = c
                    .get("bucketPolicy")
                    .and_then(Value::as_str)
                    .map(policy_allows_public_principal)
                    .unwrap_or(false);
                let actually_public = public_acl || policy_public;
                let compliant = pab || !actually_public;
                out.push(outcome(
                    ci,
                    compliant,
                    if compliant {
                        None
                    } else {
                        Some("Bucket grants public access".into())
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
    let proto = rule.get("ipProtocol").and_then(Value::as_str).unwrap_or("");
    let from = rule.get("fromPort").and_then(Value::as_i64).unwrap_or(0);
    let to = rule.get("toPort").and_then(Value::as_i64).unwrap_or(65535);
    // An all-traffic rule (`IpProtocol "-1"`, or `-1` port sentinels) opens
    // every port, so it must be treated as covering the blocked port. Without
    // this, a security group opening ALL ports to 0.0.0.0/0 would be reported
    // COMPLIANT — a false all-clear on the worst case.
    let all_ports = proto == "-1" || from == -1 || to == -1;
    let covers = all_ports || (from <= port && port <= to);
    let public = rule
        .get("ipRanges")
        .and_then(Value::as_array)
        .map(|a| a.iter().any(|c| c.as_str() == Some("0.0.0.0/0")))
        .unwrap_or(false);
    covers && public
}

/// Whether an S3 ACL grantee URI targets the public / authenticated-users
/// groups (i.e. a public grant).
fn is_public_grantee_uri(uri: Option<&str>) -> bool {
    uri.map(|u| u.contains("AllUsers") || u.contains("AuthenticatedUsers"))
        .unwrap_or(false)
}

/// Parse a bucket policy JSON document and report whether any `Allow` statement
/// grants access to the `*` principal (`"*"`, `{"AWS":"*"}`, or an array
/// containing `"*"`). Replaces the previous whitespace-sensitive substring
/// match, which missed the standard `"Principal": "*"` form the console/SDK
/// emit.
fn policy_allows_public_principal(policy: &str) -> bool {
    let Ok(doc) = serde_json::from_str::<Value>(policy) else {
        return false;
    };
    let statements = match doc.get("Statement") {
        Some(Value::Array(a)) => a.clone(),
        Some(obj @ Value::Object(_)) => vec![obj.clone()],
        _ => return false,
    };
    statements.iter().any(|stmt| {
        let effect = stmt
            .get("Effect")
            .and_then(Value::as_str)
            .unwrap_or("Allow");
        if !effect.eq_ignore_ascii_case("Allow") {
            return false;
        }
        principal_is_public(stmt.get("Principal"))
    })
}

fn principal_is_public(principal: Option<&Value>) -> bool {
    match principal {
        Some(Value::String(s)) => s == "*",
        Some(Value::Object(map)) => map.values().any(value_contains_star),
        Some(Value::Array(a)) => a.iter().any(|v| v.as_str() == Some("*")),
        _ => false,
    }
}

fn value_contains_star(v: &Value) -> bool {
    match v {
        Value::String(s) => s == "*",
        Value::Array(a) => a.iter().any(|x| x.as_str() == Some("*")),
        _ => false,
    }
}

/// Whether a configuration item falls within a config rule's `Scope`. An empty
/// or absent scope matches everything (the AWS default).
fn item_in_scope(ci: &ConfigurationItem, scope: &Value) -> bool {
    if scope.is_null() {
        return true;
    }
    if let Some(types) = scope
        .get("ComplianceResourceTypes")
        .and_then(Value::as_array)
    {
        if !types.is_empty()
            && !types
                .iter()
                .any(|t| t.as_str() == Some(ci.resource_type.as_str()))
        {
            return false;
        }
    }
    if let Some(id) = scope.get("ComplianceResourceId").and_then(Value::as_str) {
        if ci.resource_id != id {
            return false;
        }
    }
    if let Some(tag_key) = scope.get("TagKey").and_then(Value::as_str) {
        match ci.tags.get(tag_key) {
            None => return false,
            Some(v) => {
                if let Some(tag_value) = scope.get("TagValue").and_then(Value::as_str) {
                    if v != tag_value {
                        return false;
                    }
                }
            }
        }
    }
    true
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

/// Proactively evaluate a single supplied resource configuration against the
/// account's AWS-managed rules, without recording it into history. Used by
/// `StartResourceEvaluation` so a proactive/detective evaluation reports the
/// *real* compliance of the supplied config rather than a canned COMPLIANT.
///
/// `configuration` is the resource configuration document (the same shape the
/// recorder stores). Returns one [`EvaluationResult`] per managed rule that
/// applies to the resource's type. An empty result means no managed rule
/// applied — the caller reports `INSUFFICIENT_DATA`, never a fabricated pass.
pub fn evaluate_proactive(
    account: &AccountState,
    resource_type: &str,
    resource_id: &str,
    configuration: &str,
) -> Vec<EvaluationResult> {
    let now = Utc::now();
    let item = ConfigurationItem {
        version: "1.3".into(),
        account_id: String::new(),
        configuration_item_capture_time: now,
        configuration_item_status: "ResourceDiscovered".into(),
        configuration_state_id: now.timestamp_millis().to_string(),
        arn: String::new(),
        resource_type: resource_type.to_string(),
        resource_id: resource_id.to_string(),
        resource_name: None,
        aws_region: String::new(),
        availability_zone: "Regional".into(),
        resource_creation_time: None,
        tags: std::collections::BTreeMap::new(),
        configuration: configuration.to_string(),
        supplementary_configuration: std::collections::BTreeMap::new(),
        externally_recorded: true,
    };
    let mut temp = AccountState::default();
    temp.config_items
        .insert(resource_key(resource_type, resource_id), vec![item]);

    let mut results = Vec::new();
    for rule in account.rules.values() {
        let owner = rule
            .source
            .get("Owner")
            .and_then(Value::as_str)
            .unwrap_or("");
        if owner != "AWS" {
            continue;
        }
        let Some(sid) = rule.source.get("SourceIdentifier").and_then(Value::as_str) else {
            continue;
        };
        let params: Value = rule
            .input_parameters
            .as_deref()
            .and_then(|p| serde_json::from_str(p).ok())
            .unwrap_or(Value::Null);
        let scope = rule.scope.clone().unwrap_or(Value::Null);
        for o in evaluate_managed_rule(sid, &params, &scope, &temp) {
            if o.resource_id == resource_id && o.resource_type == resource_type {
                results.push(outcome_to_result(&rule.name, &o));
            }
        }
    }
    results
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{resource_key, AccountState, ConfigurationItem};
    use chrono::Utc;
    use std::collections::BTreeMap;

    fn item(rtype: &str, rid: &str, config: Value, tags: &[(&str, &str)]) -> ConfigurationItem {
        ConfigurationItem {
            version: "1.3".into(),
            account_id: "123456789012".into(),
            configuration_item_capture_time: Utc::now(),
            configuration_item_status: "OK".into(),
            configuration_state_id: "1".into(),
            arn: format!("arn:aws:::{rid}"),
            resource_type: rtype.into(),
            resource_id: rid.into(),
            resource_name: Some(rid.into()),
            aws_region: "us-east-1".into(),
            availability_zone: "Regional".into(),
            resource_creation_time: Some(Utc::now()),
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            configuration: serde_json::to_string(&config).unwrap(),
            supplementary_configuration: BTreeMap::new(),
            externally_recorded: false,
        }
    }

    fn account_with(items: Vec<ConfigurationItem>) -> AccountState {
        let mut acc = AccountState::default();
        for it in items {
            let key = resource_key(&it.resource_type, &it.resource_id);
            acc.config_items.entry(key).or_default().push(it);
        }
        acc
    }

    fn verdict(outcomes: &[RuleOutcome], rid: &str) -> Option<String> {
        outcomes
            .iter()
            .find(|o| o.resource_id == rid)
            .map(|o| o.compliance_type.clone())
    }

    #[test]
    fn default_private_bucket_is_compliant() {
        // No public grants, no bucket policy, no Public Access Block.
        let acc = account_with(vec![item(
            "AWS::S3::Bucket",
            "priv",
            json!({ "name": "priv", "grantsPublicRead": false, "grantsPublicWrite": false }),
            &[],
        )]);
        let out = evaluate_managed_rule(
            "S3_BUCKET_PUBLIC_READ_PROHIBITED",
            &Value::Null,
            &Value::Null,
            &acc,
        );
        assert_eq!(verdict(&out, "priv").as_deref(), Some("COMPLIANT"));
        let out = evaluate_managed_rule(
            "S3_BUCKET_PUBLIC_WRITE_PROHIBITED",
            &Value::Null,
            &Value::Null,
            &acc,
        );
        assert_eq!(verdict(&out, "priv").as_deref(), Some("COMPLIANT"));
    }

    #[test]
    fn public_acl_bucket_is_noncompliant() {
        let acc = account_with(vec![item(
            "AWS::S3::Bucket",
            "pub",
            json!({ "name": "pub", "grantsPublicRead": true, "grantsPublicWrite": false }),
            &[],
        )]);
        let out = evaluate_managed_rule(
            "S3_BUCKET_PUBLIC_READ_PROHIBITED",
            &Value::Null,
            &Value::Null,
            &acc,
        );
        assert_eq!(verdict(&out, "pub").as_deref(), Some("NON_COMPLIANT"));
    }

    #[test]
    fn spaced_principal_policy_detected_public() {
        // The console/SDK emit `"Principal": "*"` with a space; the old
        // substring check missed it.
        assert!(policy_allows_public_principal(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal": "*","Action":"s3:GetObject","Resource":"*"}]}"#
        ));
        assert!(policy_allows_public_principal(
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:*"}]}"#
        ));
        assert!(!policy_allows_public_principal(
            r#"{"Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123:root"},"Action":"s3:GetObject"}]}"#
        ));
        // And the rule flags a bucket whose policy uses the spaced form.
        let acc = account_with(vec![item(
            "AWS::S3::Bucket",
            "polpub",
            json!({ "name": "polpub", "bucketPolicy": "{\"Statement\":[{\"Effect\":\"Allow\",\"Principal\": \"*\",\"Action\":\"s3:GetObject\"}]}" }),
            &[],
        )]);
        let out = evaluate_managed_rule(
            "S3_BUCKET_PUBLIC_READ_PROHIBITED",
            &Value::Null,
            &Value::Null,
            &acc,
        );
        assert_eq!(verdict(&out, "polpub").as_deref(), Some("NON_COMPLIANT"));
    }

    #[test]
    fn all_ports_open_sg_is_noncompliant_for_ssh() {
        // IpProtocol "-1" with -1 port sentinels = all traffic to the world.
        let acc = account_with(vec![item(
            "AWS::EC2::SecurityGroup",
            "sg-all",
            json!({
                "groupName": "wide-open",
                "ipPermissions": [{ "ipProtocol": "-1", "fromPort": -1, "toPort": -1, "ipRanges": ["0.0.0.0/0"] }],
            }),
            &[],
        )]);
        let out = evaluate_managed_rule("INCOMING_SSH_DISABLED", &Value::Null, &Value::Null, &acc);
        assert_eq!(verdict(&out, "sg-all").as_deref(), Some("NON_COMPLIANT"));

        // A narrow non-SSH rule stays compliant for SSH.
        let acc2 = account_with(vec![item(
            "AWS::EC2::SecurityGroup",
            "sg-web",
            json!({
                "groupName": "web",
                "ipPermissions": [{ "ipProtocol": "tcp", "fromPort": 443, "toPort": 443, "ipRanges": ["0.0.0.0/0"] }],
            }),
            &[],
        )]);
        let out2 =
            evaluate_managed_rule("INCOMING_SSH_DISABLED", &Value::Null, &Value::Null, &acc2);
        assert_eq!(verdict(&out2, "sg-web").as_deref(), Some("COMPLIANT"));
    }

    #[test]
    fn required_tags_respects_scope() {
        let acc = account_with(vec![
            item("AWS::S3::Bucket", "b1", json!({ "name": "b1" }), &[]),
            item(
                "AWS::EC2::Instance",
                "i-1",
                json!({ "instanceId": "i-1" }),
                &[("Env", "prod")],
            ),
        ]);
        let params = json!({ "tag1Key": "Env" });
        let scope = json!({ "ComplianceResourceTypes": ["AWS::S3::Bucket"] });
        let out = evaluate_managed_rule("REQUIRED_TAGS", &params, &scope, &acc);
        // Only the in-scope bucket is evaluated; the out-of-scope instance is not.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].resource_id, "b1");
        assert_eq!(out[0].compliance_type, "NON_COMPLIANT"); // bucket lacks the Env tag
    }

    #[test]
    fn externally_recorded_items_are_not_delete_marked() {
        // A PutResourceConfig'd item of a native type must survive a sync that
        // does not rediscover it (no live S3/EC2/IAM states wired here).
        let mut ext = item(
            "AWS::S3::Bucket",
            "manual",
            json!({ "name": "manual" }),
            &[],
        );
        ext.externally_recorded = true;
        let mut acc = account_with(vec![ext]);
        apply_recorded_items(&mut acc, Vec::new(), "123456789012");
        let key = resource_key("AWS::S3::Bucket", "manual");
        let last = acc.config_items.get(&key).unwrap().last().unwrap();
        assert_ne!(last.configuration_item_status, "ResourceDeleted");
    }
}
