//! Security-group operations: groups, ingress/egress rules (IpPermissions),
//! rule descriptions, VPC associations, references, and stale-group queries.

use std::collections::HashMap;

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    filter_value_matches, gen_id, indexed_list, parse_filters, require, validate_max_results,
    Filter,
};
use crate::state::{Ec2State, SecurityGroup, SecurityGroupRule, SecurityGroupVpcAssociation, Tag};

fn rule_xml(r: &SecurityGroupRule, owner: &str, region: &str) -> String {
    let mut out = format!(
        "{}{}{}<isEgress>{}</isEgress>{}<fromPort>{}</fromPort><toPort>{}</toPort>{}{}",
        ec2_elem("securityGroupRuleId", &r.rule_id),
        ec2_elem("groupId", &r.group_id),
        ec2_elem("groupOwnerId", owner),
        r.is_egress,
        ec2_elem("ipProtocol", &r.ip_protocol),
        r.from_port,
        r.to_port,
        ec2_elem("description", &r.description),
        ec2_elem(
            "securityGroupRuleArn",
            &format!(
                "arn:aws:ec2:{region}:{owner}:security-group-rule/{}",
                r.rule_id
            ),
        ),
    );
    if let Some(c) = &r.cidr_ipv4 {
        out.push_str(&ec2_elem("cidrIpv4", c));
    }
    if let Some(c) = &r.cidr_ipv6 {
        out.push_str(&ec2_elem("cidrIpv6", c));
    }
    if let Some(p) = &r.prefix_list_id {
        out.push_str(&ec2_elem("prefixListId", p));
    }
    if r.referenced_group_id.is_some() || r.referenced_user_id.is_some() {
        let mut info = String::new();
        if let Some(g) = &r.referenced_group_id {
            info.push_str(&ec2_elem("groupId", g));
        }
        if let Some(u) = &r.referenced_user_id {
            info.push_str(&ec2_elem("userId", u));
        }
        out.push_str(&format!(
            "<referencedGroupInfo>{info}</referencedGroupInfo>"
        ));
    }
    out
}

/// Emit the protocol/port header of an IpPermission group.
///
/// For the "all traffic" protocol (`-1`), AWS omits FromPort/ToPort entirely;
/// the provider then normalises them to 0. Emitting `-1` here makes the
/// `aws_security_group` resource see a perpetual port diff. tcp/udp/icmp
/// always carry their port range.
fn ip_permission_header(r: &SecurityGroupRule) -> String {
    if r.ip_protocol == "-1" {
        ec2_elem("ipProtocol", &r.ip_protocol)
    } else {
        format!(
            "{}<fromPort>{}</fromPort><toPort>{}</toPort>",
            ec2_elem("ipProtocol", &r.ip_protocol),
            r.from_port,
            r.to_port,
        )
    }
}

/// Emit the range children (`ipRanges`/`ipv6Ranges`/`prefixListIds`/`groups`)
/// for a single stored rule record. Storage keeps one range source per record,
/// so each record contributes at most one range child; the aggregator
/// concatenates the children of every record that shares a permission group.
fn ip_permission_ranges(r: &SecurityGroupRule) -> String {
    let mut ranges = String::new();
    if let Some(c) = &r.cidr_ipv4 {
        ranges.push_str(&format!(
            "<ipRanges><item>{}{}</item></ipRanges>",
            ec2_elem("cidrIp", c),
            ec2_elem("description", &r.description)
        ));
    }
    if let Some(c) = &r.cidr_ipv6 {
        ranges.push_str(&format!(
            "<ipv6Ranges><item>{}{}</item></ipv6Ranges>",
            ec2_elem("cidrIpv6", c),
            ec2_elem("description", &r.description)
        ));
    }
    if let Some(p) = &r.prefix_list_id {
        ranges.push_str(&format!(
            "<prefixListIds><item>{}{}</item></prefixListIds>",
            ec2_elem("prefixListId", p),
            ec2_elem("description", &r.description)
        ));
    }
    if r.referenced_group_id.is_some() || r.referenced_group_name.is_some() {
        let mut item = String::new();
        if let Some(u) = &r.referenced_user_id {
            item.push_str(&ec2_elem("userId", u));
        }
        if let Some(g) = &r.referenced_group_id {
            item.push_str(&ec2_elem("groupId", g));
        }
        if let Some(n) = &r.referenced_group_name {
            item.push_str(&ec2_elem("groupName", n));
        }
        if !r.description.is_empty() {
            item.push_str(&ec2_elem("description", &r.description));
        }
        ranges.push_str(&format!("<groups><item>{item}</item></groups>"));
    }
    ranges
}

/// Aggregate stored rules into IpPermission `<item>` bodies for
/// DescribeSecurityGroups. Records that share the same protocol/from-port/
/// to-port collapse into ONE permission entry -- matching how real EC2 groups
/// ranges -- concatenating each record's range children. Description is
/// per-range (attached to each range child), so it is NOT part of the group
/// key. Stored order is preserved: groups appear in first-seen order and range
/// children within a group keep their stored order, making the output
/// deterministic.
fn grouped_permissions<'a>(rules: impl Iterator<Item = &'a SecurityGroupRule>) -> Vec<String> {
    // Parallel vectors keyed by (protocol, from_port, to_port); `bodies[i]` is
    // the accumulating `<item>` inner XML for `keys[i]`.
    let mut keys: Vec<(String, i64, i64)> = Vec::new();
    let mut bodies: Vec<String> = Vec::new();
    for r in rules {
        let key = (r.ip_protocol.clone(), r.from_port, r.to_port);
        let ranges = ip_permission_ranges(r);
        if let Some(pos) = keys.iter().position(|k| *k == key) {
            bodies[pos].push_str(&ranges);
        } else {
            keys.push(key);
            bodies.push(format!("{}{}", ip_permission_header(r), ranges));
        }
    }
    bodies
}

fn sg_xml(sg: &SecurityGroup, tags: &[Tag], owner: &str, region: &str) -> String {
    let ingress = grouped_permissions(sg.rules.iter().filter(|r| !r.is_egress));
    let egress = grouped_permissions(sg.rules.iter().filter(|r| r.is_egress));
    format!(
        "{}{}{}{}{}{}{}{}",
        ec2_elem("groupId", &sg.group_id),
        ec2_elem("groupName", &sg.group_name),
        ec2_elem("groupDescription", &sg.description),
        ec2_elem("ownerId", owner),
        ec2_elem("vpcId", &sg.vpc_id),
        ec2_elem(
            "securityGroupArn",
            &format!(
                "arn:aws:ec2:{region}:{owner}:security-group/{}",
                sg.group_id
            )
        ),
        format_args!(
            "{}{}",
            ec2_list("ipPermissions", &ingress),
            ec2_list("ipPermissionsEgress", &egress)
        ),
        super::tags::tag_set_xml(tags),
    )
}

/// Parse `IpPermissions.N` (+ legacy flat form) into individual stored rules.
fn parse_ip_permissions(
    params: &HashMap<String, String>,
    group_id: &str,
    is_egress: bool,
) -> Vec<SecurityGroupRule> {
    let mut out = Vec::new();
    let mut n = 1usize;
    let mut any_perm = false;
    loop {
        let proto_key = format!("IpPermissions.{n}.IpProtocol");
        if !params.contains_key(&proto_key) {
            break;
        }
        any_perm = true;
        let proto = params
            .get(&proto_key)
            .cloned()
            .unwrap_or_else(|| "-1".into());
        let from = params
            .get(&format!("IpPermissions.{n}.FromPort"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(-1);
        let to = params
            .get(&format!("IpPermissions.{n}.ToPort"))
            .and_then(|v| v.parse().ok())
            .unwrap_or(-1);
        let templ = |ref_id: RuleRef| SecurityGroupRule {
            rule_id: gen_id("sgr"),
            group_id: group_id.to_string(),
            is_egress,
            ip_protocol: proto.clone(),
            from_port: from,
            to_port: to,
            cidr_ipv4: ref_id.cidr4,
            cidr_ipv6: ref_id.cidr6,
            prefix_list_id: ref_id.prefix_list,
            referenced_group_id: ref_id.group_id,
            referenced_group_name: ref_id.group_name,
            referenced_user_id: ref_id.user_id,
            description: ref_id.description,
        };
        let mut emitted = false;
        for (cidr, desc) in
            indexed_sub_desc(params, &format!("IpPermissions.{n}.IpRanges"), "CidrIp")
        {
            out.push(templ(RuleRef {
                cidr4: Some(cidr),
                description: desc,
                ..Default::default()
            }));
            emitted = true;
        }
        for (cidr, desc) in
            indexed_sub_desc(params, &format!("IpPermissions.{n}.Ipv6Ranges"), "CidrIpv6")
        {
            out.push(templ(RuleRef {
                cidr6: Some(cidr),
                description: desc,
                ..Default::default()
            }));
            emitted = true;
        }
        for (pl, desc) in indexed_sub_desc(
            params,
            &format!("IpPermissions.{n}.PrefixListIds"),
            "PrefixListId",
        ) {
            out.push(templ(RuleRef {
                prefix_list: Some(pl),
                description: desc,
                ..Default::default()
            }));
            emitted = true;
        }
        // Source-group references carry GroupId and/or GroupName plus an
        // optional UserId (cross-account) and Description. Iterate while either
        // an id or a name is present so name-only references (default-VPC form)
        // are not dropped.
        let mut m = 1usize;
        loop {
            let gp = format!("IpPermissions.{n}.Groups.{m}");
            let gid = sub_opt(params, &format!("{gp}.GroupId"));
            let gname = sub_opt(params, &format!("{gp}.GroupName"));
            if gid.is_none() && gname.is_none() {
                break;
            }
            out.push(templ(RuleRef {
                group_id: gid,
                group_name: gname,
                user_id: sub_opt(params, &format!("{gp}.UserId")),
                description: sub_opt(params, &format!("{gp}.Description")).unwrap_or_default(),
                ..Default::default()
            }));
            emitted = true;
            m += 1;
        }
        if !emitted {
            out.push(templ(RuleRef::default()));
        }
        n += 1;
    }
    // Legacy flat single-rule form.
    if !any_perm {
        if let Some(cidr) = params.get("CidrIp").cloned() {
            out.push(SecurityGroupRule {
                rule_id: gen_id("sgr"),
                group_id: group_id.to_string(),
                is_egress,
                ip_protocol: params
                    .get("IpProtocol")
                    .cloned()
                    .unwrap_or_else(|| "-1".into()),
                from_port: params
                    .get("FromPort")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(-1),
                to_port: params
                    .get("ToPort")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(-1),
                cidr_ipv4: Some(cidr),
                cidr_ipv6: None,
                prefix_list_id: None,
                referenced_group_id: None,
                referenced_group_name: None,
                referenced_user_id: None,
                description: String::new(),
            });
        }
    }
    out
}

/// The reference target of a single parsed IpPermission sub-element, used to
/// build one `SecurityGroupRule` without a wide positional constructor.
#[derive(Default)]
struct RuleRef {
    cidr4: Option<String>,
    cidr6: Option<String>,
    prefix_list: Option<String>,
    group_id: Option<String>,
    group_name: Option<String>,
    user_id: Option<String>,
    description: String,
}

/// Read `key`, returning `None` when absent or empty.
fn sub_opt(params: &HashMap<String, String>, key: &str) -> Option<String> {
    params.get(key).filter(|v| !v.is_empty()).cloned()
}

/// Collect `{prefix}.M.{field}` (paired with a sibling `Description`) for
/// M = 1.. . Returns each value with its description (empty when absent).
fn indexed_sub_desc(
    params: &HashMap<String, String>,
    prefix: &str,
    field: &str,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut m = 1usize;
    loop {
        match params.get(&format!("{prefix}.{m}.{field}")) {
            Some(v) if !v.is_empty() => {
                let desc = params
                    .get(&format!("{prefix}.{m}.Description"))
                    .cloned()
                    .unwrap_or_default();
                out.push((v.clone(), desc));
            }
            _ => break,
        }
        m += 1;
    }
    out
}

pub(crate) fn create_security_group(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let description = require(&req.query_params, "GroupDescription")
        .or_else(|_| require(&req.query_params, "Description"))?;
    let name = require(&req.query_params, "GroupName")?;
    let vpc_id = req.query_params.get("VpcId").cloned().unwrap_or_default();
    let group_id = gen_id("sg");
    // Default egress: allow all outbound.
    let egress = SecurityGroupRule {
        rule_id: gen_id("sgr"),
        group_id: group_id.clone(),
        is_egress: true,
        ip_protocol: "-1".to_string(),
        from_port: -1,
        to_port: -1,
        cidr_ipv4: Some("0.0.0.0/0".to_string()),
        cidr_ipv6: None,
        prefix_list_id: None,
        referenced_group_id: None,
        referenced_group_name: None,
        referenced_user_id: None,
        description: String::new(),
    };
    let sg = SecurityGroup {
        group_id: group_id.clone(),
        group_name: name,
        description,
        vpc_id,
        rules: vec![egress],
    };
    let owner = req.account_id.clone();
    let region = req.region.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &group_id,
            "security-group",
        );
        let t = state.tags_for(&group_id).to_vec();
        state.security_groups.insert(group_id.clone(), sg);
        t
    };
    let body = format!(
        "{}{}{}",
        ec2_elem("groupId", &group_id),
        ec2_elem(
            "securityGroupArn",
            &format!("arn:aws:ec2:{region}:{owner}:security-group/{group_id}")
        ),
        super::tags::tag_set_xml(&tags),
    );
    Ok(Ec2Service::respond(
        "CreateSecurityGroup",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_security_group(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // The VPC's `default` security group cannot be deleted — AWS returns
    // `CannotDelete`. Without this guard, deleting it left a VPC with no
    // default group, so a later no-SecurityGroupId RunInstances launched with
    // an empty group list (impossible on AWS) (bug-hunt 2026-06-18 finding,
    // delete-protection). Resolve the target group(s) and reject the default.
    let targets: Vec<&crate::state::SecurityGroup> =
        if let Some(id) = req.query_params.get("GroupId") {
            state.security_groups.get(id).into_iter().collect()
        } else if let Some(name) = req.query_params.get("GroupName") {
            state
                .security_groups
                .values()
                .filter(|g| &g.group_name == name)
                .collect()
        } else {
            Vec::new()
        };
    if targets.iter().any(|g| g.group_name == "default") {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "CannotDelete",
            "the default security group cannot be deleted",
        ));
    }

    if let Some(id) = req.query_params.get("GroupId") {
        state.security_groups.remove(id);
        state.tags.remove(id);
    } else if let Some(name) = req.query_params.get("GroupName") {
        let ids: Vec<String> = state
            .security_groups
            .values()
            .filter(|g| &g.group_name == name)
            .map(|g| g.group_id.clone())
            .collect();
        for id in ids {
            state.security_groups.remove(&id);
        }
    }
    Ok(Ec2Service::respond(
        "DeleteSecurityGroup",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_security_groups(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let filters = parse_filters(&req.query_params);
    let wanted_ids = indexed_list(&req.query_params, "GroupId");
    let wanted_names = indexed_list(&req.query_params, "GroupName");
    let owner = req.account_id.clone();
    let region = req.region.clone();

    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);

    // An explicitly-requested group id / name that does not exist is a hard
    // error on AWS (InvalidGroup.NotFound), not a silently-empty result.
    for id in &wanted_ids {
        if !state.security_groups.contains_key(id) {
            return Err(sg_not_found(id));
        }
    }
    for name in &wanted_names {
        if !state
            .security_groups
            .values()
            .any(|g| &g.group_name == name)
        {
            return Err(sg_not_found(name));
        }
    }

    let mut items: Vec<String> = state
        .security_groups
        .values()
        .filter(|g| wanted_ids.is_empty() || wanted_ids.contains(&g.group_id))
        .filter(|g| wanted_names.is_empty() || wanted_names.contains(&g.group_name))
        .filter(|g| sg_matches(g, state.tags_for(&g.group_id), &filters))
        .map(|g| sg_xml(g, state.tags_for(&g.group_id), &owner, &region))
        .collect();
    items.sort();
    let max_results = req
        .query_params
        .get("MaxResults")
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<usize>().ok());
    let next_token = req.query_params.get("NextToken").map(String::as_str);
    let (page, token) = crate::service_helpers::paginate(&items, next_token, max_results);
    let body = format!(
        "{}{}",
        ec2_list("securityGroupInfo", &page),
        token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default(),
    );
    Ok(Ec2Service::respond(
        "DescribeSecurityGroups",
        &req.request_id,
        &body,
    ))
}

fn sg_matches(g: &SecurityGroup, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "group-id" => vec![g.group_id.clone()],
            "group-name" => vec![g.group_name.clone()],
            "vpc-id" => vec![g.vpc_id.clone()],
            "description" => vec![g.description.clone()],
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            "tag-value" => tags.iter().map(|t| t.value.clone()).collect(),
            name => {
                if let Some(key) = name.strip_prefix("tag:") {
                    tags.iter()
                        .filter(|t| t.key == key)
                        .map(|t| t.value.clone())
                        .collect()
                } else {
                    // Unknown filter: match nothing (never silently match all).
                    return false;
                }
            }
        };
        f.values
            .iter()
            .any(|v| candidates.iter().any(|c| filter_value_matches(v, c)))
    })
}

/// `InvalidGroup.NotFound` — the referenced security group does not exist.
fn sg_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "InvalidGroup.NotFound",
        format!("The security group '{id}' does not exist"),
    )
}

/// Two rules describe the same permission when protocol, port range, and the
/// single source/target (cidr/prefix-list/referenced-group) all match. Used to
/// revoke exactly the supplied IpPermissions instead of a whole direction.
fn same_permission(a: &SecurityGroupRule, b: &SecurityGroupRule) -> bool {
    a.is_egress == b.is_egress
        && a.ip_protocol == b.ip_protocol
        && ports_match(a, b)
        && a.cidr_ipv4 == b.cidr_ipv4
        && a.cidr_ipv6 == b.cidr_ipv6
        && a.prefix_list_id == b.prefix_list_id
        && a.referenced_group_id == b.referenced_group_id
}

/// Ports match for revoke purposes. For the all-traffic protocol (`-1`) AWS
/// ignores port numbers entirely — it stores none — but clients round-trip the
/// pair as `-1/-1`, `0/0`, or absent interchangeably (terraform revokes the
/// default egress rule with `FromPort=0`/`ToPort=0`). So when the protocol is
/// `-1` the ports are irrelevant to identity; for any real protocol they must
/// match exactly.
fn ports_match(a: &SecurityGroupRule, b: &SecurityGroupRule) -> bool {
    if a.ip_protocol == "-1" {
        return true;
    }
    a.from_port == b.from_port && a.to_port == b.to_port
}

fn authorize(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
    is_egress: bool,
) -> Result<AwsResponse, AwsServiceError> {
    let group_id = if is_egress {
        require(&req.query_params, "GroupId")?
    } else {
        req.query_params.get("GroupId").cloned().unwrap_or_default()
    };
    let new_rules = parse_ip_permissions(&req.query_params, &group_id, is_egress);
    let owner = req.account_id.clone();
    let region = req.region.clone();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // A missing or empty GroupId (or a nonexistent one) is a hard error on
        // AWS, not a silent no-op.
        let sg = state
            .security_groups
            .get_mut(&group_id)
            .ok_or_else(|| sg_not_found(&group_id))?;
        sg.rules.extend(new_rules.clone());
    }
    // New rules change what traffic is allowed — re-apply the firewall (ph3).
    svc.spawn_firewall_reconcile();
    let rule_items: Vec<String> = new_rules
        .iter()
        .map(|r| rule_xml(r, &owner, &region))
        .collect();
    let body = format!(
        "{}{}",
        ec2_return(true),
        ec2_list("securityGroupRuleSet", &rule_items)
    );
    Ok(Ec2Service::respond(action, &req.request_id, &body))
}

pub(crate) fn authorize_security_group_ingress(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    authorize(svc, req, "AuthorizeSecurityGroupIngress", false)
}

pub(crate) fn authorize_security_group_egress(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    authorize(svc, req, "AuthorizeSecurityGroupEgress", true)
}

fn revoke(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
    is_egress: bool,
) -> Result<AwsResponse, AwsServiceError> {
    let group_id = if is_egress {
        require(&req.query_params, "GroupId")?
    } else {
        req.query_params.get("GroupId").cloned().unwrap_or_default()
    };
    let rule_ids = indexed_list(&req.query_params, "SecurityGroupRuleId");
    let described = parse_ip_permissions(&req.query_params, &group_id, is_egress);
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // A missing/empty/nonexistent GroupId is a hard error on AWS.
        let sg = state
            .security_groups
            .get_mut(&group_id)
            .ok_or_else(|| sg_not_found(&group_id))?;
        if !rule_ids.is_empty() {
            sg.rules.retain(|r| !rule_ids.contains(&r.rule_id));
        } else {
            // Revoke ONLY the specific permissions described in the request,
            // matched by protocol/port/source — never the whole direction.
            sg.rules
                .retain(|r| !described.iter().any(|d| same_permission(r, d)));
        }
    }
    // Removing rules tightens the firewall — re-apply (ph3).
    svc.spawn_firewall_reconcile();
    Ok(Ec2Service::respond(
        action,
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn revoke_security_group_ingress(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    revoke(svc, req, "RevokeSecurityGroupIngress", false)
}

pub(crate) fn revoke_security_group_egress(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    revoke(svc, req, "RevokeSecurityGroupEgress", true)
}

pub(crate) fn describe_security_group_rules(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "SecurityGroupRuleId");
    let owner = req.account_id.clone();
    let region = req.region.clone();

    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .security_groups
        .values()
        .flat_map(|g| g.rules.iter())
        .filter(|r| wanted.is_empty() || wanted.contains(&r.rule_id))
        .map(|r| rule_xml(r, &owner, &region))
        .collect();
    items.sort();
    let body = ec2_list("securityGroupRuleSet", &items);
    Ok(Ec2Service::respond(
        "DescribeSecurityGroupRules",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn modify_security_group_rules(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let group_id = require(&req.query_params, "GroupId")?;
    let p = &req.query_params;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let sg = state.security_groups.get_mut(&group_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                http::StatusCode::BAD_REQUEST,
                "InvalidGroup.NotFound",
                format!("The security group '{group_id}' does not exist"),
            )
        })?;
        let mut n = 1usize;
        loop {
            let id_key = format!("SecurityGroupRule.{n}.SecurityGroupRuleId");
            let Some(rule_id) = p.get(&id_key) else { break };
            let pre = format!("SecurityGroupRule.{n}.SecurityGroupRule");
            if let Some(rule) = sg.rules.iter_mut().find(|r| &r.rule_id == rule_id) {
                if let Some(v) = p.get(&format!("{pre}.IpProtocol")) {
                    rule.ip_protocol = v.clone();
                }
                if let Some(v) = p
                    .get(&format!("{pre}.FromPort"))
                    .and_then(|v| v.parse().ok())
                {
                    rule.from_port = v;
                }
                if let Some(v) = p.get(&format!("{pre}.ToPort")).and_then(|v| v.parse().ok()) {
                    rule.to_port = v;
                }
                if let Some(v) = p.get(&format!("{pre}.CidrIpv4")) {
                    rule.cidr_ipv4 = Some(v.clone());
                    rule.cidr_ipv6 = None;
                    rule.prefix_list_id = None;
                    rule.referenced_group_id = None;
                    rule.referenced_group_name = None;
                    rule.referenced_user_id = None;
                }
                if let Some(v) = p.get(&format!("{pre}.CidrIpv6")) {
                    rule.cidr_ipv6 = Some(v.clone());
                    rule.cidr_ipv4 = None;
                    rule.prefix_list_id = None;
                    rule.referenced_group_id = None;
                    rule.referenced_group_name = None;
                    rule.referenced_user_id = None;
                }
                if let Some(v) = p.get(&format!("{pre}.PrefixListId")) {
                    rule.prefix_list_id = Some(v.clone());
                    rule.cidr_ipv4 = None;
                    rule.cidr_ipv6 = None;
                    rule.referenced_group_id = None;
                    rule.referenced_group_name = None;
                    rule.referenced_user_id = None;
                }
                if let Some(v) = p.get(&format!("{pre}.ReferencedGroupId")) {
                    rule.referenced_group_id = Some(v.clone());
                    rule.cidr_ipv4 = None;
                    rule.cidr_ipv6 = None;
                    rule.prefix_list_id = None;
                }
                if let Some(v) = p.get(&format!("{pre}.Description")) {
                    rule.description = v.clone();
                }
            }
            n += 1;
        }
    }
    // Rule changes alter allowed traffic — re-apply the firewall (ph3).
    svc.spawn_firewall_reconcile();
    Ok(Ec2Service::respond(
        "ModifySecurityGroupRules",
        &req.request_id,
        &ec2_return(true),
    ))
}

/// Match existing rules by protocol/port/CIDR and overwrite their descriptions.
/// Shared by the ingress and egress `UpdateSecurityGroupRuleDescriptions*` ops.
fn update_rule_descriptions(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
    is_egress: bool,
) -> Result<AwsResponse, AwsServiceError> {
    let group_id = require(&req.query_params, "GroupId")?;
    let p = &req.query_params;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let sg = state.security_groups.get_mut(&group_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                http::StatusCode::BAD_REQUEST,
                "InvalidGroup.NotFound",
                format!("The security group '{group_id}' does not exist"),
            )
        })?;
        {
            let mut n = 1usize;
            loop {
                let proto_key = format!("IpPermissions.{n}.IpProtocol");
                if !p.contains_key(&proto_key) {
                    break;
                }
                let proto = p.get(&proto_key).cloned().unwrap_or_else(|| "-1".into());
                let from = p
                    .get(&format!("IpPermissions.{n}.FromPort"))
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(-1);
                let to = p
                    .get(&format!("IpPermissions.{n}.ToPort"))
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(-1);
                let mut m = 1usize;
                loop {
                    let cidr_key = format!("IpPermissions.{n}.IpRanges.{m}.CidrIp");
                    let cidr6_key = format!("IpPermissions.{n}.Ipv6Ranges.{m}.CidrIpv6");
                    let (cidr4, cidr6) = (p.get(&cidr_key), p.get(&cidr6_key));
                    if cidr4.is_none() && cidr6.is_none() {
                        break;
                    }
                    let desc = p
                        .get(&format!("IpPermissions.{n}.IpRanges.{m}.Description"))
                        .or_else(|| p.get(&format!("IpPermissions.{n}.Ipv6Ranges.{m}.Description")))
                        .cloned()
                        .unwrap_or_default();
                    for rule in sg.rules.iter_mut().filter(|r| {
                        r.is_egress == is_egress
                            && r.ip_protocol == proto
                            && r.from_port == from
                            && r.to_port == to
                            && ((cidr4.is_some()
                                && r.cidr_ipv4.as_deref() == cidr4.map(|s| s.as_str()))
                                || (cidr6.is_some()
                                    && r.cidr_ipv6.as_deref() == cidr6.map(|s| s.as_str())))
                    }) {
                        rule.description = desc.clone();
                    }
                    m += 1;
                }
                n += 1;
            }
        }
    }
    Ok(Ec2Service::respond(
        action,
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn update_rule_descriptions_ingress(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    update_rule_descriptions(
        svc,
        req,
        "UpdateSecurityGroupRuleDescriptionsIngress",
        false,
    )
}

pub(crate) fn update_rule_descriptions_egress(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    update_rule_descriptions(svc, req, "UpdateSecurityGroupRuleDescriptionsEgress", true)
}

fn sg_vpc_assoc_key(group_id: &str, vpc_id: &str) -> String {
    format!("{group_id}:{vpc_id}")
}

fn sg_vpc_assoc_xml(a: &SecurityGroupVpcAssociation, owner: &str) -> String {
    format!(
        "{}{}{}{}{}",
        ec2_elem("groupId", &a.group_id),
        ec2_elem("vpcId", &a.vpc_id),
        ec2_elem("vpcOwnerId", owner),
        ec2_elem("state", &a.state),
        ec2_elem("groupOwnerId", owner),
    )
}

pub(crate) fn associate_security_group_vpc(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let group_id = require(&req.query_params, "GroupId")?;
    let vpc_id = require(&req.query_params, "VpcId")?;
    let assoc = SecurityGroupVpcAssociation {
        group_id: group_id.clone(),
        vpc_id: vpc_id.clone(),
        state: "associated".to_string(),
    };
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .security_group_vpc_associations
            .insert(sg_vpc_assoc_key(&group_id, &vpc_id), assoc);
    }
    Ok(Ec2Service::respond(
        "AssociateSecurityGroupVpc",
        &req.request_id,
        &ec2_elem("state", "associated"),
    ))
}

pub(crate) fn disassociate_security_group_vpc(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let group_id = require(&req.query_params, "GroupId")?;
    let vpc_id = require(&req.query_params, "VpcId")?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .security_group_vpc_associations
            .remove(&sg_vpc_assoc_key(&group_id, &vpc_id));
    }
    Ok(Ec2Service::respond(
        "DisassociateSecurityGroupVpc",
        &req.request_id,
        &ec2_elem("state", "disassociated"),
    ))
}

pub(crate) fn describe_security_group_vpc_associations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let filters = parse_filters(&req.query_params);
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .security_group_vpc_associations
        .values()
        .filter(|a| {
            filters.iter().all(|f| match f.name.as_str() {
                "group-id" => f.values.contains(&a.group_id),
                "vpc-id" => f.values.contains(&a.vpc_id),
                "state" => f.values.contains(&a.state),
                _ => true,
            })
        })
        .map(|a| sg_vpc_assoc_xml(a, &owner))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeSecurityGroupVpcAssociations",
        &req.request_id,
        &ec2_list("securityGroupVpcAssociationSet", &items),
    ))
}

pub(crate) fn get_security_groups_for_vpc(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let vpc_id = require(&req.query_params, "VpcId")?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .security_groups
        .values()
        .filter(|g| g.vpc_id == vpc_id)
        .map(|g| {
            format!(
                "{}{}{}{}{}",
                ec2_elem("groupId", &g.group_id),
                ec2_elem("groupName", &g.group_name),
                ec2_elem("description", &g.description),
                ec2_elem("ownerId", &owner),
                ec2_elem("primaryVpcId", &vpc_id),
            )
        })
        .collect();
    let body = ec2_list("securityGroupForVpcSet", &items);
    Ok(Ec2Service::respond(
        "GetSecurityGroupsForVpc",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_stale_security_groups(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 255)?;
    crate::service_helpers::validate_length(&req.query_params, "NextToken", 1, 1024)?;
    require(&req.query_params, "VpcId")?;
    Ok(Ec2Service::respond(
        "DescribeStaleSecurityGroups",
        &req.request_id,
        &ec2_list("staleSecurityGroupSet", &[]),
    ))
}

pub(crate) fn describe_security_group_references(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // GroupId is a required *list* (`GroupId.N`); list omission is not
    // wire-observable, so it is not validated here. Returns an empty
    // reference set (no cross-VPC references are modeled).
    Ok(Ec2Service::respond(
        "DescribeSecurityGroupReferences",
        &req.request_id,
        &ec2_list("securityGroupReferenceSet", &[]),
    ))
}

#[cfg(test)]
mod modify_tests {
    use super::*;

    fn req(action: &str, query: &[(&str, &str)]) -> AwsRequest {
        AwsRequest {
            service: "ec2".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    fn seed_group(svc: &Ec2Service, rule: SecurityGroupRule) -> String {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("000000000000");
        state.security_groups.insert(
            "sg-1".to_string(),
            SecurityGroup {
                group_id: "sg-1".into(),
                group_name: "g".into(),
                description: "d".into(),
                vpc_id: "vpc-1".into(),
                rules: vec![rule],
            },
        );
        "sg-1".to_string()
    }

    fn base_rule() -> SecurityGroupRule {
        SecurityGroupRule {
            rule_id: "sgr-1".into(),
            group_id: "sg-1".into(),
            is_egress: false,
            ip_protocol: "tcp".into(),
            from_port: 22,
            to_port: 22,
            cidr_ipv4: Some("10.0.0.0/8".into()),
            cidr_ipv6: None,
            prefix_list_id: None,
            referenced_group_id: None,
            referenced_group_name: None,
            referenced_user_id: None,
            description: "old".into(),
        }
    }

    #[test]
    fn modify_security_group_rules_updates_rule() {
        let svc = Ec2Service::new();
        seed_group(&svc, base_rule());
        modify_security_group_rules(
            &svc,
            &req(
                "ModifySecurityGroupRules",
                &[
                    ("GroupId", "sg-1"),
                    ("SecurityGroupRule.1.SecurityGroupRuleId", "sgr-1"),
                    ("SecurityGroupRule.1.SecurityGroupRule.IpProtocol", "tcp"),
                    ("SecurityGroupRule.1.SecurityGroupRule.FromPort", "443"),
                    ("SecurityGroupRule.1.SecurityGroupRule.ToPort", "443"),
                    (
                        "SecurityGroupRule.1.SecurityGroupRule.CidrIpv4",
                        "0.0.0.0/0",
                    ),
                    ("SecurityGroupRule.1.SecurityGroupRule.Description", "https"),
                ],
            ),
        )
        .unwrap();

        let accounts = svc.state.read();
        let r = &accounts.get("000000000000").unwrap().security_groups["sg-1"].rules[0];
        assert_eq!(r.from_port, 443);
        assert_eq!(r.to_port, 443);
        assert_eq!(r.cidr_ipv4.as_deref(), Some("0.0.0.0/0"));
        assert_eq!(r.description, "https");
    }

    fn ingress_rule(id: &str, port: i64, cidr: &str) -> SecurityGroupRule {
        SecurityGroupRule {
            rule_id: id.into(),
            group_id: "sg-1".into(),
            is_egress: false,
            ip_protocol: "tcp".into(),
            from_port: port,
            to_port: port,
            cidr_ipv4: Some(cidr.into()),
            cidr_ipv6: None,
            prefix_list_id: None,
            referenced_group_id: None,
            referenced_group_name: None,
            referenced_user_id: None,
            description: String::new(),
        }
    }

    fn seed_group_rules(svc: &Ec2Service, rules: Vec<SecurityGroupRule>) {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("000000000000");
        state.security_groups.insert(
            "sg-1".to_string(),
            SecurityGroup {
                group_id: "sg-1".into(),
                group_name: "g".into(),
                description: "d".into(),
                vpc_id: "vpc-1".into(),
                rules,
            },
        );
    }

    #[test]
    fn revoke_by_ip_permission_removes_only_the_matching_rule() {
        let svc = Ec2Service::new();
        seed_group_rules(
            &svc,
            vec![
                ingress_rule("sgr-a", 22, "10.0.0.0/8"),
                ingress_rule("sgr-b", 443, "0.0.0.0/0"),
            ],
        );
        revoke_security_group_ingress(
            &svc,
            &req(
                "RevokeSecurityGroupIngress",
                &[
                    ("GroupId", "sg-1"),
                    ("IpPermissions.1.IpProtocol", "tcp"),
                    ("IpPermissions.1.FromPort", "22"),
                    ("IpPermissions.1.ToPort", "22"),
                    ("IpPermissions.1.IpRanges.1.CidrIp", "10.0.0.0/8"),
                ],
            ),
        )
        .unwrap();

        let accounts = svc.state.read();
        let rules = &accounts.get("000000000000").unwrap().security_groups["sg-1"].rules;
        assert_eq!(rules.len(), 1, "only the port-22 rule should be revoked");
        assert_eq!(rules[0].rule_id, "sgr-b");
    }

    #[test]
    fn revoke_all_traffic_egress_ignores_port_representation() {
        // Terraform revokes the AWS-created default egress rule (protocol -1,
        // stored as -1/-1) by sending FromPort=0/ToPort=0. The revoke must still
        // match and remove it, or `aws_security_group` shows egress.# = 1.
        let svc = Ec2Service::new();
        let resp = create_security_group(
            &svc,
            &req(
                "CreateSecurityGroup",
                &[
                    ("GroupName", "t"),
                    ("GroupDescription", "d"),
                    ("VpcId", "vpc-1"),
                ],
            ),
        )
        .unwrap();
        let sg_id = {
            let body = String::from_utf8_lossy(resp.body.expect_bytes());
            body.split("<groupId>")
                .nth(1)
                .and_then(|s| s.split("</groupId>").next())
                .unwrap()
                .to_string()
        };
        revoke_security_group_egress(
            &svc,
            &req(
                "RevokeSecurityGroupEgress",
                &[
                    ("GroupId", &sg_id),
                    ("IpPermissions.1.IpProtocol", "-1"),
                    ("IpPermissions.1.FromPort", "0"),
                    ("IpPermissions.1.ToPort", "0"),
                    ("IpPermissions.1.IpRanges.1.CidrIp", "0.0.0.0/0"),
                ],
            ),
        )
        .unwrap();

        let accounts = svc.state.read();
        let rules = &accounts.get("000000000000").unwrap().security_groups[&sg_id].rules;
        assert!(
            rules.iter().all(|r| !r.is_egress),
            "default all-traffic egress rule should be revoked despite 0/0 ports"
        );
    }

    #[test]
    fn authorize_missing_group_errors() {
        let svc = Ec2Service::new();
        let err = crate::test_support::err_of(authorize_security_group_ingress(
            &svc,
            &req(
                "AuthorizeSecurityGroupIngress",
                &[
                    ("IpPermissions.1.IpProtocol", "tcp"),
                    ("IpPermissions.1.FromPort", "22"),
                    ("IpPermissions.1.ToPort", "22"),
                    ("IpPermissions.1.IpRanges.1.CidrIp", "0.0.0.0/0"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidGroup.NotFound");
    }

    #[test]
    fn revoke_missing_group_errors() {
        let svc = Ec2Service::new();
        let err = crate::test_support::err_of(revoke_security_group_ingress(
            &svc,
            &req("RevokeSecurityGroupIngress", &[("GroupId", "sg-nope")]),
        ));
        assert_eq!(err.code(), "InvalidGroup.NotFound");
    }

    #[test]
    fn describe_unknown_filter_matches_nothing() {
        let svc = Ec2Service::new();
        seed_group_rules(&svc, vec![]);
        let resp = describe_security_groups(
            &svc,
            &req(
                "DescribeSecurityGroups",
                &[
                    ("Filter.1.Name", "not-a-real-filter"),
                    ("Filter.1.Value.1", "whatever"),
                ],
            ),
        )
        .unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(
            !body.contains("<groupId>sg-1</groupId>"),
            "unknown filter must not match all: {body}"
        );
    }

    #[test]
    fn describe_tag_wildcard_matches() {
        let svc = Ec2Service::new();
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_or_create("000000000000");
            state.security_groups.insert(
                "sg-prod".into(),
                SecurityGroup {
                    group_id: "sg-prod".into(),
                    group_name: "prod".into(),
                    description: "d".into(),
                    vpc_id: "vpc-1".into(),
                    rules: vec![],
                },
            );
            state.upsert_tags(
                "sg-prod",
                &[Tag {
                    key: "Name".into(),
                    value: "prod-web".into(),
                }],
            );
            state.security_groups.insert(
                "sg-dev".into(),
                SecurityGroup {
                    group_id: "sg-dev".into(),
                    group_name: "dev".into(),
                    description: "d".into(),
                    vpc_id: "vpc-1".into(),
                    rules: vec![],
                },
            );
            state.upsert_tags(
                "sg-dev",
                &[Tag {
                    key: "Name".into(),
                    value: "dev-web".into(),
                }],
            );
        }
        let resp = describe_security_groups(
            &svc,
            &req(
                "DescribeSecurityGroups",
                &[
                    ("Filter.1.Name", "tag:Name"),
                    ("Filter.1.Value.1", "prod-*"),
                ],
            ),
        )
        .unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(body.contains("<groupId>sg-prod</groupId>"), "{body}");
        assert!(!body.contains("<groupId>sg-dev</groupId>"), "{body}");
    }

    #[test]
    fn update_rule_descriptions_sets_description_by_match() {
        let svc = Ec2Service::new();
        seed_group(&svc, base_rule());
        update_rule_descriptions_ingress(
            &svc,
            &req(
                "UpdateSecurityGroupRuleDescriptionsIngress",
                &[
                    ("GroupId", "sg-1"),
                    ("IpPermissions.1.IpProtocol", "tcp"),
                    ("IpPermissions.1.FromPort", "22"),
                    ("IpPermissions.1.ToPort", "22"),
                    ("IpPermissions.1.IpRanges.1.CidrIp", "10.0.0.0/8"),
                    ("IpPermissions.1.IpRanges.1.Description", "ssh from vpc"),
                ],
            ),
        )
        .unwrap();

        let accounts = svc.state.read();
        let r = &accounts.get("000000000000").unwrap().security_groups["sg-1"].rules[0];
        assert_eq!(r.description, "ssh from vpc");
    }

    fn empty_group(svc: &Ec2Service) {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create("000000000000")
            .security_groups
            .insert(
                "sg-1".to_string(),
                SecurityGroup {
                    group_id: "sg-1".into(),
                    group_name: "g".into(),
                    description: "d".into(),
                    vpc_id: "vpc-1".into(),
                    rules: Vec::new(),
                },
            );
    }

    #[test]
    fn authorize_ingress_persists_iprange_description() {
        let svc = Ec2Service::new();
        empty_group(&svc);
        authorize_security_group_ingress(
            &svc,
            &req(
                "AuthorizeSecurityGroupIngress",
                &[
                    ("GroupId", "sg-1"),
                    ("IpPermissions.1.IpProtocol", "tcp"),
                    ("IpPermissions.1.FromPort", "443"),
                    ("IpPermissions.1.ToPort", "443"),
                    ("IpPermissions.1.IpRanges.1.CidrIp", "10.0.0.0/8"),
                    ("IpPermissions.1.IpRanges.1.Description", "https from vpc"),
                ],
            ),
        )
        .unwrap();

        // The stored rule carries the inline description...
        {
            let accounts = svc.state.read();
            let rules = &accounts.get("000000000000").unwrap().security_groups["sg-1"].rules;
            let r = rules.iter().find(|r| !r.is_egress).unwrap();
            assert_eq!(r.description, "https from vpc");
        }
        // ...and DescribeSecurityGroups renders it in the ipRanges item.
        let resp = describe_security_groups(&svc, &req("DescribeSecurityGroups", &[])).unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(
            body.contains("<description>https from vpc</description>"),
            "describe body missing inline description: {body}"
        );
    }

    #[test]
    fn describe_aggregates_v4_and_v6_ranges_into_one_permission() {
        let svc = Ec2Service::new();
        empty_group(&svc);
        // One IpPermission carrying BOTH an IpRange and an Ipv6Range.
        authorize_security_group_egress(
            &svc,
            &req(
                "AuthorizeSecurityGroupEgress",
                &[
                    ("GroupId", "sg-1"),
                    ("IpPermissions.1.IpProtocol", "-1"),
                    ("IpPermissions.1.IpRanges.1.CidrIp", "0.0.0.0/0"),
                    ("IpPermissions.1.Ipv6Ranges.1.CidrIpv6", "::/0"),
                ],
            ),
        )
        .unwrap();

        let resp = describe_security_groups(&svc, &req("DescribeSecurityGroups", &[])).unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();

        // Isolate the egress permission set.
        let egress = body
            .split("<ipPermissionsEgress>")
            .nth(1)
            .and_then(|s| s.split("</ipPermissionsEgress>").next())
            .expect("no ipPermissionsEgress in describe body");

        // Exactly ONE permission <item> (aggregated), not two.
        assert_eq!(
            egress.matches("<item>").count(),
            // one permission item + one ipRanges item + one ipv6Ranges item
            3,
            "expected a single aggregated egress permission, got: {egress}"
        );
        // The single permission carries BOTH range families.
        assert!(
            egress.contains("<ipRanges><item><cidrIp>0.0.0.0/0</cidrIp>"),
            "aggregated permission missing ipRanges: {egress}"
        );
        assert!(
            egress.contains("<ipv6Ranges><item><cidrIpv6>::/0</cidrIpv6>"),
            "aggregated permission missing ipv6Ranges: {egress}"
        );
    }

    #[test]
    fn describe_keeps_distinct_protocols_as_separate_permissions() {
        let svc = Ec2Service::new();
        empty_group(&svc);
        // Two distinct protocol/port permissions must NOT be over-aggregated.
        authorize_security_group_ingress(
            &svc,
            &req(
                "AuthorizeSecurityGroupIngress",
                &[
                    ("GroupId", "sg-1"),
                    ("IpPermissions.1.IpProtocol", "tcp"),
                    ("IpPermissions.1.FromPort", "22"),
                    ("IpPermissions.1.ToPort", "22"),
                    ("IpPermissions.1.IpRanges.1.CidrIp", "10.0.0.0/8"),
                    ("IpPermissions.2.IpProtocol", "tcp"),
                    ("IpPermissions.2.FromPort", "443"),
                    ("IpPermissions.2.ToPort", "443"),
                    ("IpPermissions.2.IpRanges.1.CidrIp", "0.0.0.0/0"),
                ],
            ),
        )
        .unwrap();

        let resp = describe_security_groups(&svc, &req("DescribeSecurityGroups", &[])).unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        let ingress = body
            .split("<ipPermissions>")
            .nth(1)
            .and_then(|s| s.split("</ipPermissions>").next())
            .expect("no ipPermissions in describe body");

        // Two separate permission items, one per port.
        assert!(
            ingress.contains("<fromPort>22</fromPort><toPort>22</toPort>"),
            "missing port-22 permission: {ingress}"
        );
        assert!(
            ingress.contains("<fromPort>443</fromPort><toPort>443</toPort>"),
            "missing port-443 permission: {ingress}"
        );
        // Two <fromPort> occurrences => two distinct permission groups.
        assert_eq!(
            ingress.matches("<fromPort>").count(),
            2,
            "distinct protocol/port permissions were over-aggregated: {ingress}"
        );
    }

    #[test]
    fn authorize_ingress_persists_group_name_and_user_id() {
        let svc = Ec2Service::new();
        empty_group(&svc);
        authorize_security_group_ingress(
            &svc,
            &req(
                "AuthorizeSecurityGroupIngress",
                &[
                    ("GroupId", "sg-1"),
                    ("IpPermissions.1.IpProtocol", "-1"),
                    ("IpPermissions.1.Groups.1.GroupName", "peer-sg"),
                    ("IpPermissions.1.Groups.1.UserId", "111122223333"),
                    ("IpPermissions.1.Groups.1.Description", "from peer"),
                ],
            ),
        )
        .unwrap();
        let accounts = svc.state.read();
        let rules = &accounts.get("000000000000").unwrap().security_groups["sg-1"].rules;
        let r = rules
            .iter()
            .find(|r| r.referenced_group_name.is_some())
            .expect("group-name reference dropped");
        assert_eq!(r.referenced_group_name.as_deref(), Some("peer-sg"));
        assert_eq!(r.referenced_user_id.as_deref(), Some("111122223333"));
        assert_eq!(r.description, "from peer");
    }

    #[test]
    fn security_group_vpc_association_round_trips() {
        let svc = Ec2Service::new();
        associate_security_group_vpc(
            &svc,
            &req(
                "AssociateSecurityGroupVpc",
                &[("GroupId", "sg-1"), ("VpcId", "vpc-abc")],
            ),
        )
        .unwrap();

        let resp = describe_security_group_vpc_associations(
            &svc,
            &req("DescribeSecurityGroupVpcAssociations", &[]),
        )
        .unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(body.contains("<groupId>sg-1</groupId>"), "{body}");
        assert!(body.contains("<vpcId>vpc-abc</vpcId>"), "{body}");
        assert!(body.contains("<state>associated</state>"), "{body}");

        // Disassociate removes it from the describe.
        disassociate_security_group_vpc(
            &svc,
            &req(
                "DisassociateSecurityGroupVpc",
                &[("GroupId", "sg-1"), ("VpcId", "vpc-abc")],
            ),
        )
        .unwrap();
        let resp = describe_security_group_vpc_associations(
            &svc,
            &req("DescribeSecurityGroupVpcAssociations", &[]),
        )
        .unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(!body.contains("vpc-abc"), "association not removed: {body}");
    }

    #[test]
    fn describe_security_groups_explicit_missing_id_errors() {
        let svc = Ec2Service::new();
        seed_group(&svc, base_rule());
        let err = crate::test_support::err_of(describe_security_groups(
            &svc,
            &req("DescribeSecurityGroups", &[("GroupId.1", "sg-missing")]),
        ));
        assert_eq!(err.code(), "InvalidGroup.NotFound");
    }

    #[test]
    fn describe_security_groups_explicit_missing_name_errors() {
        let svc = Ec2Service::new();
        seed_group(&svc, base_rule());
        let err = crate::test_support::err_of(describe_security_groups(
            &svc,
            &req("DescribeSecurityGroups", &[("GroupName.1", "nope")]),
        ));
        assert_eq!(err.code(), "InvalidGroup.NotFound");
    }

    #[test]
    fn describe_security_groups_tag_value_filter() {
        let svc = Ec2Service::new();
        seed_group(&svc, base_rule());
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_or_create("000000000000");
            state.tags.insert(
                "sg-1".to_string(),
                vec![Tag {
                    key: "team".into(),
                    value: "core".into(),
                }],
            );
        }
        let resp = describe_security_groups(
            &svc,
            &req(
                "DescribeSecurityGroups",
                &[("Filter.1.Name", "tag-value"), ("Filter.1.Value.1", "core")],
            ),
        )
        .unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(body.contains("<groupId>sg-1</groupId>"), "{body}");
    }
}
