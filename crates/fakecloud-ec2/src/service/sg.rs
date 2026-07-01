//! Security-group operations: groups, ingress/egress rules (IpPermissions),
//! rule descriptions, VPC associations, references, and stale-group queries.

use std::collections::HashMap;

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, parse_filters, require, validate_max_results, Filter,
};
use crate::state::{Ec2State, SecurityGroup, SecurityGroupRule, Tag};

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
    if let Some(g) = &r.referenced_group_id {
        out.push_str(&format!(
            "<referencedGroupInfo>{}</referencedGroupInfo>",
            ec2_elem("groupId", g)
        ));
    }
    out
}

/// Render one `<item>` IpPermission from a rule.
fn ip_permission_xml(r: &SecurityGroupRule) -> String {
    // For the "all traffic" protocol (`-1`), AWS omits FromPort/ToPort entirely;
    // the provider then normalises them to 0. Emitting `-1` here makes the
    // `aws_security_group` resource see a perpetual port diff. tcp/udp/icmp
    // always carry their port range.
    let mut inner = if r.ip_protocol == "-1" {
        ec2_elem("ipProtocol", &r.ip_protocol)
    } else {
        format!(
            "{}<fromPort>{}</fromPort><toPort>{}</toPort>",
            ec2_elem("ipProtocol", &r.ip_protocol),
            r.from_port,
            r.to_port,
        )
    };
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
            "<ipv6Ranges><item>{}</item></ipv6Ranges>",
            ec2_elem("cidrIpv6", c)
        ));
    }
    if let Some(p) = &r.prefix_list_id {
        ranges.push_str(&format!(
            "<prefixListIds><item>{}</item></prefixListIds>",
            ec2_elem("prefixListId", p)
        ));
    }
    if let Some(g) = &r.referenced_group_id {
        ranges.push_str(&format!(
            "<groups><item>{}</item></groups>",
            ec2_elem("groupId", g)
        ));
    }
    inner.push_str(&ranges);
    inner
}

fn sg_xml(sg: &SecurityGroup, tags: &[Tag], owner: &str, region: &str) -> String {
    let ingress: Vec<String> = sg
        .rules
        .iter()
        .filter(|r| !r.is_egress)
        .map(ip_permission_xml)
        .collect();
    let egress: Vec<String> = sg
        .rules
        .iter()
        .filter(|r| r.is_egress)
        .map(ip_permission_xml)
        .collect();
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
        let base = |cidr4, cidr6, pl, grp, desc| SecurityGroupRule {
            rule_id: gen_id("sgr"),
            group_id: group_id.to_string(),
            is_egress,
            ip_protocol: proto.clone(),
            from_port: from,
            to_port: to,
            cidr_ipv4: cidr4,
            cidr_ipv6: cidr6,
            prefix_list_id: pl,
            referenced_group_id: grp,
            description: desc,
        };
        let mut emitted = false;
        for cidr in indexed_sub(params, &format!("IpPermissions.{n}.IpRanges"), "CidrIp") {
            out.push(base(Some(cidr), None, None, None, String::new()));
            emitted = true;
        }
        for cidr in indexed_sub(params, &format!("IpPermissions.{n}.Ipv6Ranges"), "CidrIpv6") {
            out.push(base(None, Some(cidr), None, None, String::new()));
            emitted = true;
        }
        for pl in indexed_sub(
            params,
            &format!("IpPermissions.{n}.PrefixListIds"),
            "PrefixListId",
        ) {
            out.push(base(None, None, Some(pl), None, String::new()));
            emitted = true;
        }
        for grp in indexed_sub(params, &format!("IpPermissions.{n}.Groups"), "GroupId") {
            out.push(base(None, None, None, Some(grp), String::new()));
            emitted = true;
        }
        if !emitted {
            out.push(base(None, None, None, None, String::new()));
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
                description: String::new(),
            });
        }
    }
    out
}

/// Collect `{prefix}.M.{field}` for M = 1.. .
fn indexed_sub(params: &HashMap<String, String>, prefix: &str, field: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut m = 1usize;
    loop {
        match params.get(&format!("{prefix}.{m}.{field}")) {
            Some(v) if !v.is_empty() => out.push(v.clone()),
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

    let mut items: Vec<String> = state
        .security_groups
        .values()
        .filter(|g| wanted_ids.is_empty() || wanted_ids.contains(&g.group_id))
        .filter(|g| wanted_names.is_empty() || wanted_names.contains(&g.group_name))
        .filter(|g| sg_matches(g, state.tags_for(&g.group_id), &filters))
        .map(|g| sg_xml(g, state.tags_for(&g.group_id), &owner, &region))
        .collect();
    items.sort();
    let body = ec2_list("securityGroupInfo", &items);
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
            name => {
                if let Some(key) = name.strip_prefix("tag:") {
                    tags.iter()
                        .filter(|t| t.key == key)
                        .map(|t| t.value.clone())
                        .collect()
                } else {
                    return true;
                }
            }
        };
        f.values.iter().any(|v| candidates.iter().any(|c| c == v))
    })
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
        if let Some(sg) = state.security_groups.get_mut(&group_id) {
            sg.rules.extend(new_rules.clone());
        }
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
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(sg) = state.security_groups.get_mut(&group_id) {
            if !rule_ids.is_empty() {
                sg.rules.retain(|r| !rule_ids.contains(&r.rule_id));
            } else {
                // Revoke by matching the supplied IpPermissions on egress flag.
                sg.rules.retain(|r| r.is_egress != is_egress);
            }
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
                }
                if let Some(v) = p.get(&format!("{pre}.CidrIpv6")) {
                    rule.cidr_ipv6 = Some(v.clone());
                    rule.cidr_ipv4 = None;
                    rule.prefix_list_id = None;
                    rule.referenced_group_id = None;
                }
                if let Some(v) = p.get(&format!("{pre}.PrefixListId")) {
                    rule.prefix_list_id = Some(v.clone());
                    rule.cidr_ipv4 = None;
                    rule.cidr_ipv6 = None;
                    rule.referenced_group_id = None;
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

pub(crate) fn associate_security_group_vpc(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "GroupId")?;
    require(&req.query_params, "VpcId")?;
    Ok(Ec2Service::respond(
        "AssociateSecurityGroupVpc",
        &req.request_id,
        &ec2_elem("state", "associated"),
    ))
}

pub(crate) fn disassociate_security_group_vpc(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "GroupId")?;
    require(&req.query_params, "VpcId")?;
    Ok(Ec2Service::respond(
        "DisassociateSecurityGroupVpc",
        &req.request_id,
        &ec2_elem("state", "disassociated"),
    ))
}

pub(crate) fn describe_security_group_vpc_associations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeSecurityGroupVpcAssociations",
        &req.request_id,
        &ec2_list("securityGroupVpcAssociationSet", &[]),
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
}
