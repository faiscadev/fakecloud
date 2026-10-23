//! Security-group operations: groups, ingress/egress rules (IpPermissions),
//! rule descriptions, VPC associations, references, and stale-group queries.

use std::collections::HashMap;

use fakecloud_aws::ec2query::{ec2_bool, ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::quota::{check_group_count, rules_limit_exceeded, GroupHolder, RuleWeights};
use crate::service::resource_quotas as rq;
use crate::service::Ec2Service;
use crate::service_helpers::{
    ec2_arn, filter_value_matches, gen_id, indexed_list, invalid_parameter_value,
    missing_parameter, parse_filters, require, validate_max_results, Filter,
};
use crate::state::{Ec2State, SecurityGroup, SecurityGroupRule, SecurityGroupVpcAssociation, Tag};

fn rule_xml(r: &SecurityGroupRule, tags: &[Tag], owner: &str, region: &str) -> String {
    let mut out = format!(
        "{}{}{}<isEgress>{}</isEgress>{}<fromPort>{}</fromPort><toPort>{}</toPort>{}{}{}",
        ec2_elem("securityGroupRuleId", &r.rule_id),
        ec2_elem("groupId", &r.group_id),
        ec2_elem("groupOwnerId", owner),
        r.is_egress,
        ec2_elem("ipProtocol", &r.ip_protocol),
        r.from_port,
        r.to_port,
        description_elem(&r.description),
        ec2_elem(
            "securityGroupRuleArn",
            &ec2_arn(region, owner, &format!("security-group-rule/{}", r.rule_id)),
        ),
        super::tags::tag_set_xml(tags),
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
        // A same-account reference still names its owning account.
        info.push_str(&ec2_elem(
            "userId",
            r.referenced_user_id.as_deref().unwrap_or(owner),
        ));
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

/// Optional `<description>` leaf: EC2 omits the member when no description was
/// set rather than emitting an empty string.
fn description_elem(description: &str) -> String {
    if description.is_empty() {
        String::new()
    } else {
        ec2_elem("description", description)
    }
}

/// The range children of one IpPermission, one `<item>` per stored rule record,
/// bucketed by list so each list renders under a SINGLE wrapper element.
#[derive(Default)]
struct PermissionRanges {
    ipv4: Vec<String>,
    ipv6: Vec<String>,
    prefix_lists: Vec<String>,
    groups: Vec<String>,
}

impl PermissionRanges {
    /// Add the range item of a single stored rule record. Storage keeps one
    /// range source per record, so each record contributes one item.
    fn push(&mut self, r: &SecurityGroupRule, owner: &str) {
        if let Some(c) = &r.cidr_ipv4 {
            self.ipv4.push(format!(
                "{}{}",
                ec2_elem("cidrIp", c),
                description_elem(&r.description)
            ));
        }
        if let Some(c) = &r.cidr_ipv6 {
            self.ipv6.push(format!(
                "{}{}",
                ec2_elem("cidrIpv6", c),
                description_elem(&r.description)
            ));
        }
        if let Some(p) = &r.prefix_list_id {
            self.prefix_lists.push(format!(
                "{}{}",
                ec2_elem("prefixListId", p),
                description_elem(&r.description)
            ));
        }
        if r.referenced_group_id.is_some() || r.referenced_group_name.is_some() {
            // A same-account pair still names its owning account.
            let mut item = ec2_elem("userId", r.referenced_user_id.as_deref().unwrap_or(owner));
            if let Some(g) = &r.referenced_group_id {
                item.push_str(&ec2_elem("groupId", g));
            }
            if let Some(n) = &r.referenced_group_name {
                item.push_str(&ec2_elem("groupName", n));
            }
            item.push_str(&description_elem(&r.description));
            self.groups.push(item);
        }
    }

    /// Render each non-empty list under one wrapper. Repeating a wrapper per
    /// item (`<ipRanges><item/></ipRanges><ipRanges><item/></ipRanges>`) is not
    /// a valid EC2 list: botocore decodes it as a list of empty structs and
    /// the Rust SDK keeps only one of the ranges.
    fn xml(&self) -> String {
        let mut out = String::new();
        for (name, items) in [
            ("ipRanges", &self.ipv4),
            ("ipv6Ranges", &self.ipv6),
            ("prefixListIds", &self.prefix_lists),
            ("groups", &self.groups),
        ] {
            if !items.is_empty() {
                out.push_str(&ec2_list(name, items));
            }
        }
        out
    }
}

/// Aggregate stored rules into IpPermission `<item>` bodies for
/// DescribeSecurityGroups. Records that share the same protocol/from-port/
/// to-port collapse into ONE permission entry -- matching how real EC2 groups
/// ranges -- with every record's range item gathered under the permission's
/// single `ipRanges`/`ipv6Ranges`/`prefixListIds`/`groups` list. Description is
/// per-range (attached to each range item), so it is NOT part of the group
/// key. Stored order is preserved: groups appear in first-seen order and range
/// items within a list keep their stored order, making the output
/// deterministic.
fn grouped_permissions<'a>(
    rules: impl Iterator<Item = &'a SecurityGroupRule>,
    owner: &str,
) -> Vec<String> {
    // Parallel vectors keyed by (protocol, from_port, to_port); `groups[i]`
    // holds the permission header and accumulated ranges for `keys[i]`.
    let mut keys: Vec<(String, i64, i64)> = Vec::new();
    let mut groups: Vec<(String, PermissionRanges)> = Vec::new();
    for r in rules {
        let key = (r.ip_protocol.clone(), r.from_port, r.to_port);
        let pos = match keys.iter().position(|k| *k == key) {
            Some(pos) => pos,
            None => {
                keys.push(key);
                groups.push((ip_permission_header(r), PermissionRanges::default()));
                groups.len() - 1
            }
        };
        groups[pos].1.push(r, owner);
    }
    groups
        .into_iter()
        .map(|(header, ranges)| format!("{header}{}", ranges.xml()))
        .collect()
}

fn sg_xml(sg: &SecurityGroup, tags: &[Tag], owner: &str, region: &str) -> String {
    let ingress = grouped_permissions(sg.rules.iter().filter(|r| !r.is_egress), owner);
    let egress = grouped_permissions(sg.rules.iter().filter(|r| r.is_egress), owner);
    format!(
        "{}{}{}{}{}{}{}{}",
        ec2_elem("groupId", &sg.group_id),
        ec2_elem("groupName", &sg.group_name),
        ec2_elem("groupDescription", &sg.description),
        ec2_elem("ownerId", owner),
        ec2_elem("vpcId", &sg.vpc_id),
        ec2_elem(
            "securityGroupArn",
            &ec2_arn(region, owner, &format!("security-group/{}", sg.group_id))
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
    // Without a `VpcId` the group is created in the account's default VPC,
    // as on AWS.
    let vpc_id = match req.query_params.get("VpcId").filter(|v| !v.is_empty()) {
        Some(v) => v.clone(),
        None => {
            let mut accounts = svc.state.write();
            let state = accounts.regional_mut(&req.account_id, &req.region);
            state
                .vpcs
                .values()
                .find(|v| v.is_default)
                .map(|v| v.vpc_id.clone())
                .unwrap_or_default()
        }
    };
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
    let limit =
        svc.enforced_count_quota(&req.account_id, &req.region, rq::SECURITY_GROUPS_PER_REGION);
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        rq::check(
            limit,
            rq::security_groups(state),
            1,
            "SecurityGroupLimitExceeded",
            |_| "The maximum number of security groups has been reached.".to_string(),
        )?;
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
            &ec2_arn(&region, &owner, &format!("security-group/{group_id}"))
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
    let state = accounts.regional_mut(&req.account_id, &req.region);
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

    let ids: Vec<String> = targets.iter().map(|g| g.group_id.clone()).collect();
    for id in ids {
        // The group's tags and its rules' tags go with it.
        if let Some(sg) = state.security_groups.remove(&id) {
            for r in &sg.rules {
                state.tags.remove(&r.rule_id);
            }
        }
        state.tags.remove(&id);
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);

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
        .filter(|g| sg_matches(g, state.tags_for(&g.group_id), &filters, &owner))
        .map(|g| sg_xml(g, state.tags_for(&g.group_id), &owner, &region))
        .collect();
    items.sort();
    let max_results = req
        .query_params
        .get("MaxResults")
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<usize>().ok());
    let next_token = req.query_params.get("NextToken").map(String::as_str);
    let (page, token) = crate::service_helpers::paginate(&items, next_token, max_results)?;
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

fn sg_matches(g: &SecurityGroup, tags: &[Tag], filters: &[Filter], owner: &str) -> bool {
    // `ip-permission.*` filters match against the group's ingress rules on AWS
    // (egress rules are matched by the separate `egress.ip-permission.*` family).
    let ingress = || g.rules.iter().filter(|r| !r.is_egress);
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "group-id" => vec![g.group_id.clone()],
            "group-name" => vec![g.group_name.clone()],
            "vpc-id" => vec![g.vpc_id.clone()],
            "description" => vec![g.description.clone()],
            "owner-id" => vec![owner.to_string()],
            "ip-permission.cidr" => ingress().filter_map(|r| r.cidr_ipv4.clone()).collect(),
            "ip-permission.ipv6-cidr" => ingress().filter_map(|r| r.cidr_ipv6.clone()).collect(),
            "ip-permission.protocol" => ingress().map(|r| r.ip_protocol.clone()).collect(),
            "ip-permission.from-port" => ingress().map(|r| r.from_port.to_string()).collect(),
            "ip-permission.to-port" => ingress().map(|r| r.to_port.to_string()).collect(),
            "ip-permission.group-id" => ingress()
                .filter_map(|r| r.referenced_group_id.clone())
                .collect(),
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
    // Resolved before the EC2 lock is taken: Service Quotas answers it.
    let rule_limit = svc.enforced_rules_per_security_group(&req.account_id, &req.region);
    let rule_items = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        // A missing or empty GroupId (or a nonexistent one) is a hard error on
        // AWS, not a silent no-op.
        let weights = RuleWeights::new(&state.managed_prefix_lists, &req.region);
        let sg = state
            .security_groups
            .get_mut(&group_id)
            .ok_or_else(|| sg_not_found(&group_id))?;
        let after: Vec<SecurityGroupRule> =
            sg.rules.iter().chain(new_rules.iter()).cloned().collect();
        weights.check_edit(&group_id, &sg.rules, &after, rule_limit)?;
        sg.rules = after;
        // `TagSpecification.N.ResourceType=security-group-rule` tags every
        // rule the request creates.
        for r in &new_rules {
            crate::service::tags::apply_tag_specifications(
                state,
                &req.query_params,
                &r.rule_id,
                "security-group-rule",
            );
        }
        new_rules
            .iter()
            .map(|r| rule_xml(r, state.tags_for(&r.rule_id), &owner, &region))
            .collect::<Vec<String>>()
    };
    // New rules change what traffic is allowed — re-apply the firewall (ph3).
    svc.spawn_firewall_reconcile();
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
        // A missing/empty/nonexistent GroupId is a hard error on AWS.
        let sg = state
            .security_groups
            .get_mut(&group_id)
            .ok_or_else(|| sg_not_found(&group_id))?;
        let before: Vec<String> = sg.rules.iter().map(|r| r.rule_id.clone()).collect();
        if !rule_ids.is_empty() {
            sg.rules.retain(|r| !rule_ids.contains(&r.rule_id));
        } else {
            // Revoke ONLY the specific permissions described in the request,
            // matched by protocol/port/source — never the whole direction.
            sg.rules
                .retain(|r| !described.iter().any(|d| same_permission(r, d)));
        }
        let kept: Vec<&str> = sg.rules.iter().map(|r| r.rule_id.as_str()).collect();
        let removed: Vec<String> = before
            .into_iter()
            .filter(|id| !kept.contains(&id.as_str()))
            .collect();
        for id in removed {
            state.tags.remove(&id);
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
    let filters = parse_filters(&req.query_params);
    let owner = req.account_id.clone();
    let region = req.region.clone();

    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let mut items: Vec<String> = state
        .security_groups
        .values()
        .flat_map(|g| g.rules.iter())
        .filter(|r| wanted.is_empty() || wanted.contains(&r.rule_id))
        .filter(|r| rule_matches(r, state.tags_for(&r.rule_id), &filters))
        .map(|r| rule_xml(r, state.tags_for(&r.rule_id), &owner, &region))
        .collect();
    items.sort();
    // MaxResults/NextToken paging is applied by the dispatch-level pagination
    // table.
    let body = ec2_list("securityGroupRuleSet", &items);
    Ok(Ec2Service::respond(
        "DescribeSecurityGroupRules",
        &req.request_id,
        &body,
    ))
}

/// DescribeSecurityGroupRules filters: `group-id`, `security-group-rule-id`,
/// and the tag family (`tag:<key>`, `tag-key`, `tag-value`).
fn rule_matches(r: &SecurityGroupRule, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "group-id" => vec![r.group_id.clone()],
            "security-group-rule-id" => vec![r.rule_id.clone()],
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

pub(crate) fn modify_security_group_rules(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let group_id = require(&req.query_params, "GroupId")?;
    let p = &req.query_params;
    let rule_limit = svc.enforced_rules_per_security_group(&req.account_id, &req.region);
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let weights = RuleWeights::new(&state.managed_prefix_lists, &req.region);
        let sg = state.security_groups.get_mut(&group_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                http::StatusCode::BAD_REQUEST,
                "InvalidGroup.NotFound",
                format!("The security group '{group_id}' does not exist"),
            )
        })?;
        // Edits apply to a copy first: moving a rule between IPv4 and IPv6 can
        // push that side over the rules-per-group quota, and a rejected request
        // must change nothing.
        let mut rules = sg.rules.clone();
        let mut n = 1usize;
        loop {
            let id_key = format!("SecurityGroupRule.{n}.SecurityGroupRuleId");
            let Some(rule_id) = p.get(&id_key) else { break };
            let pre = format!("SecurityGroupRule.{n}.SecurityGroupRule");
            if let Some(rule) = rules.iter_mut().find(|r| &r.rule_id == rule_id) {
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
        weights.check_edit(&group_id, &sg.rules, &rules, rule_limit)?;
        sg.rules = rules;
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
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
            .regional_mut(&req.account_id, &req.region)
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
            .regional_mut(&req.account_id, &req.region)
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
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

// ---- quota validation ----

/// `ValidateSecurityGroupQuotasForInterface`: answer whether the requested set
/// of security groups could be attached to a single network interface without
/// breaching a VPC quota.
///
/// The answer is derived from the stored groups, not assumed: every id is
/// resolved (an unknown one is `InvalidGroup.NotFound`, exactly as the rest of
/// the security-group surface reports it), and the rule counts that feed the
/// quota arithmetic are the groups' real ingress/egress rules. AWS returns
/// `valid=true` or an error -- there is no "false" answer -- so each quota it
/// documents gets its own error code and message here.
pub(crate) fn validate_security_group_quotas_for_interface(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let group_ids = indexed_list(&req.query_params, "SecurityGroupId");
    // Unlike the describe ops, an empty list here is not "match everything":
    // there is nothing to validate, and AWS requires at least one id.
    if group_ids.is_empty() {
        return Err(missing_parameter("SecurityGroupId"));
    }
    // "each ID must be unique" -- a repeat is rejected rather than silently
    // deduplicated, which would validate a smaller set than the caller sent.
    for (i, id) in group_ids.iter().enumerate() {
        if group_ids[..i].contains(id) {
            return Err(invalid_parameter_value(format!(
                "The security group ID '{id}' may only be specified once"
            )));
        }
    }

    // Resolve every id against stored state and take its real rule counts. A
    // missing group is an error, so no quota arithmetic ever runs over a group
    // that was assumed to be empty.
    let mut counts: Vec<(String, usize)> = Vec::with_capacity(group_ids.len());
    {
        let accounts = svc.state.read();
        let empty = Ec2State::new(&req.account_id, &req.region);
        let state = accounts
            .regional(&req.account_id, &req.region)
            .unwrap_or(&empty);
        let weights = RuleWeights::new(&state.managed_prefix_lists, &req.region);
        for id in &group_ids {
            let sg = state
                .security_groups
                .get(id)
                .ok_or_else(|| sg_not_found(id))?;
            counts.push((id.clone(), weights.group_rule_count(&sg.rules)));
        }
    }

    // Validation answers against the applied values whether or not they are
    // enforced: asking whether groups fit the quotas is what it is for.
    check_group_count(
        Some(svc.security_groups_per_interface(&req.account_id, &req.region)),
        GroupHolder::Interface,
        group_ids.len(),
    )?;
    let rule_limit = svc.rules_per_security_group(&req.account_id, &req.region);
    for (id, worst) in &counts {
        // Each direction has its own allowance, so the busier one decides.
        if *worst > rule_limit {
            return Err(rules_limit_exceeded(format!(
                "The security group '{id}' has {worst} rules in one direction, exceeding the \
                 limit of {rule_limit} rules per security group"
            )));
        }
    }
    // A DryRun runs the same validation -- including the quota arithmetic, so a
    // set that would be rejected is still rejected -- and reports nothing back,
    // matching how the rest of EC2 treats one.
    if req
        .query_params
        .get("DryRun")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    {
        return Ok(Ec2Service::respond(
            "ValidateSecurityGroupQuotasForInterface",
            &req.request_id,
            "",
        ));
    }
    Ok(Ec2Service::respond(
        "ValidateSecurityGroupQuotasForInterface",
        &req.request_id,
        &ec2_bool("valid", true),
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
        let state = accounts.regional_mut("000000000000", "us-east-1");
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
        let r = &accounts
            .regional("000000000000", "us-east-1")
            .unwrap()
            .security_groups["sg-1"]
            .rules[0];
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
        let state = accounts.regional_mut("000000000000", "us-east-1");
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
        let rules = &accounts
            .regional("000000000000", "us-east-1")
            .unwrap()
            .security_groups["sg-1"]
            .rules;
        assert_eq!(rules.len(), 1, "only the port-22 rule should be revoked");
        assert_eq!(rules[0].rule_id, "sgr-b");
    }

    #[test]
    fn create_security_group_without_vpc_lands_in_default_vpc() {
        let svc = Ec2Service::new();
        let resp = create_security_group(
            &svc,
            &req(
                "CreateSecurityGroup",
                &[("GroupName", "novpc"), ("GroupDescription", "d")],
            ),
        )
        .unwrap();
        let body = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
        let sg_id = body
            .split("<groupId>")
            .nth(1)
            .and_then(|s| s.split("</groupId>").next())
            .unwrap()
            .to_string();
        let accounts = svc.state.read();
        let state = accounts.regional("000000000000", "us-east-1").unwrap();
        let default_vpc = state.vpcs.values().find(|v| v.is_default).unwrap();
        assert_eq!(state.security_groups[&sg_id].vpc_id, default_vpc.vpc_id);
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
        let rules = &accounts
            .regional("000000000000", "us-east-1")
            .unwrap()
            .security_groups[&sg_id]
            .rules;
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
            let state = accounts.regional_mut("000000000000", "us-east-1");
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
        let r = &accounts
            .regional("000000000000", "us-east-1")
            .unwrap()
            .security_groups["sg-1"]
            .rules[0];
        assert_eq!(r.description, "ssh from vpc");
    }

    fn empty_group(svc: &Ec2Service) {
        let mut accounts = svc.state.write();
        accounts
            .regional_mut("000000000000", "us-east-1")
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
            let rules = &accounts
                .regional("000000000000", "us-east-1")
                .unwrap()
                .security_groups["sg-1"]
                .rules;
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
    fn describe_lists_every_range_of_a_permission_under_one_wrapper() {
        let svc = Ec2Service::new();
        empty_group(&svc);
        // Two CIDRs (with descriptions) plus one without, all in one permission.
        authorize_security_group_ingress(
            &svc,
            &req(
                "AuthorizeSecurityGroupIngress",
                &[
                    ("GroupId", "sg-1"),
                    ("IpPermissions.1.IpProtocol", "tcp"),
                    ("IpPermissions.1.FromPort", "22"),
                    ("IpPermissions.1.ToPort", "22"),
                    ("IpPermissions.1.IpRanges.1.CidrIp", "172.0.0.0/12"),
                    ("IpPermissions.1.IpRanges.1.Description", "Allow SSH"),
                    ("IpPermissions.1.IpRanges.2.CidrIp", "192.168.0.0/16"),
                    ("IpPermissions.1.IpRanges.2.Description", "Allow SSH"),
                    ("IpPermissions.1.IpRanges.3.CidrIp", "10.0.0.0/8"),
                ],
            ),
        )
        .unwrap();

        let body = sg_describe_body(&svc, &[]);
        let ingress = body
            .split("<ipPermissions>")
            .nth(1)
            .and_then(|s| s.split("</ipPermissions>").next())
            .expect("no ipPermissions in describe body");
        assert_eq!(
            ingress,
            "<item><ipProtocol>tcp</ipProtocol><fromPort>22</fromPort><toPort>22</toPort>\
             <ipRanges>\
             <item><cidrIp>172.0.0.0/12</cidrIp><description>Allow SSH</description></item>\
             <item><cidrIp>192.168.0.0/16</cidrIp><description>Allow SSH</description></item>\
             <item><cidrIp>10.0.0.0/8</cidrIp></item>\
             </ipRanges></item>"
        );
    }

    #[test]
    fn same_account_group_references_name_the_owning_account() {
        let svc = Ec2Service::new();
        let mut rule = base_rule();
        rule.cidr_ipv4 = None;
        rule.referenced_group_id = Some("sg-1".into());
        rule.description = String::new();
        seed_group(&svc, rule);

        let body = sg_describe_body(&svc, &[("GroupId.1", "sg-1")]);
        assert!(
            body.contains(
                "<groups><item><userId>000000000000</userId><groupId>sg-1</groupId></item></groups>"
            ),
            "{body}"
        );
        let body = rules_describe_body(&svc, &[("SecurityGroupRuleId.1", "sgr-1")]);
        assert!(
            body.contains(
                "<referencedGroupInfo><groupId>sg-1</groupId><userId>000000000000</userId></referencedGroupInfo>"
            ),
            "{body}"
        );
    }

    fn rules_describe_body(svc: &Ec2Service, query: &[(&str, &str)]) -> String {
        let resp =
            describe_security_group_rules(svc, &req("DescribeSecurityGroupRules", query)).unwrap();
        String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
    }

    fn seed_two_groups(svc: &Ec2Service) {
        let mut a = ingress_rule("sgr-a", 22, "10.0.0.0/8");
        a.group_id = "sg-a".into();
        let mut b = ingress_rule("sgr-b", 443, "0.0.0.0/0");
        b.group_id = "sg-b".into();
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut("000000000000", "us-east-1");
        for (id, rule) in [("sg-a", a), ("sg-b", b)] {
            state.security_groups.insert(
                id.to_string(),
                SecurityGroup {
                    group_id: id.into(),
                    group_name: id.into(),
                    description: "d".into(),
                    vpc_id: "vpc-1".into(),
                    rules: vec![rule],
                },
            );
        }
    }

    #[test]
    fn describe_security_group_rules_filters_by_group_and_rule_id() {
        let svc = Ec2Service::new();
        seed_two_groups(&svc);

        let body = rules_describe_body(
            &svc,
            &[("Filter.1.Name", "group-id"), ("Filter.1.Value.1", "sg-a")],
        );
        assert!(body.contains("sgr-a"), "{body}");
        assert!(!body.contains("sgr-b"), "group-id filter leaked: {body}");
        // Only sg-a's rule matches: the account's default-VPC group rules too
        // are filtered out.
        assert_eq!(body.matches("<securityGroupRuleId>").count(), 1, "{body}");

        let body = rules_describe_body(
            &svc,
            &[
                ("Filter.1.Name", "security-group-rule-id"),
                ("Filter.1.Value.1", "sgr-b"),
            ],
        );
        assert_eq!(body.matches("<securityGroupRuleId>").count(), 1, "{body}");
        assert!(body.contains("sgr-b"), "{body}");

        let body = rules_describe_body(
            &svc,
            &[("Filter.1.Name", "bogus"), ("Filter.1.Value.1", "x")],
        );
        assert!(!body.contains("<securityGroupRuleId>"), "{body}");
    }

    #[test]
    fn security_group_rule_tags_round_trip_and_filter() {
        let svc = Ec2Service::new();
        empty_group(&svc);
        let resp = authorize_security_group_ingress(
            &svc,
            &req(
                "AuthorizeSecurityGroupIngress",
                &[
                    ("GroupId", "sg-1"),
                    ("IpPermissions.1.IpProtocol", "tcp"),
                    ("IpPermissions.1.FromPort", "22"),
                    ("IpPermissions.1.ToPort", "22"),
                    ("IpPermissions.1.IpRanges.1.CidrIp", "10.0.0.0/8"),
                    ("TagSpecification.1.ResourceType", "security-group-rule"),
                    ("TagSpecification.1.Tag.1.Key", "team"),
                    ("TagSpecification.1.Tag.1.Value", "net"),
                ],
            ),
        )
        .unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(
            body.contains("<tagSet><item><key>team</key><value>net</value></item></tagSet>"),
            "authorize response missing rule tags: {body}"
        );
        // No description was given: the member is omitted, not emitted empty.
        assert!(!body.contains("<description>"), "{body}");

        let body = rules_describe_body(
            &svc,
            &[("Filter.1.Name", "tag:team"), ("Filter.1.Value.1", "net")],
        );
        assert_eq!(body.matches("<securityGroupRuleId>").count(), 1, "{body}");
        assert!(body.contains("<key>team</key>"), "{body}");
        let rule_id = body
            .split("<securityGroupRuleId>")
            .nth(1)
            .and_then(|s| s.split('<').next())
            .unwrap()
            .to_string();

        // Revoking the rule drops its tags with it.
        revoke_security_group_ingress(
            &svc,
            &req(
                "RevokeSecurityGroupIngress",
                &[("GroupId", "sg-1"), ("SecurityGroupRuleId.1", &rule_id)],
            ),
        )
        .unwrap();
        let accounts = svc.state.read();
        assert!(accounts
            .regional("000000000000", "us-east-1")
            .unwrap()
            .tags_for(&rule_id)
            .is_empty());
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
        let rules = &accounts
            .regional("000000000000", "us-east-1")
            .unwrap()
            .security_groups["sg-1"]
            .rules;
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
            let state = accounts.regional_mut("000000000000", "us-east-1");
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

    fn sg_describe_body(svc: &Ec2Service, query: &[(&str, &str)]) -> String {
        let resp = describe_security_groups(svc, &req("DescribeSecurityGroups", query)).unwrap();
        String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
    }

    #[test]
    fn describe_security_groups_owner_id_filter() {
        let svc = Ec2Service::new();
        seed_group(&svc, base_rule());
        // owner-id matches the requesting account.
        assert!(sg_describe_body(
            &svc,
            &[
                ("Filter.1.Name", "owner-id"),
                ("Filter.1.Value.1", "000000000000")
            ],
        )
        .contains("<groupId>sg-1</groupId>"));
        // A different account filters it out.
        assert!(!sg_describe_body(
            &svc,
            &[
                ("Filter.1.Name", "owner-id"),
                ("Filter.1.Value.1", "999999999999")
            ],
        )
        .contains("<groupId>sg-1</groupId>"));
    }

    #[test]
    fn describe_security_groups_ip_permission_filters() {
        let svc = Ec2Service::new();
        // Ingress tcp/22 from 10.0.0.0/8, plus a referenced-group rule.
        let mut ref_rule = ingress_rule("sgr-ref", 443, "0.0.0.0/0");
        ref_rule.cidr_ipv4 = None;
        ref_rule.referenced_group_id = Some("sg-peer".into());
        seed_group_rules(
            &svc,
            vec![ingress_rule("sgr-a", 22, "10.0.0.0/8"), ref_rule],
        );

        assert!(sg_describe_body(
            &svc,
            &[
                ("Filter.1.Name", "ip-permission.cidr"),
                ("Filter.1.Value.1", "10.0.0.0/8"),
            ],
        )
        .contains("<groupId>sg-1</groupId>"));
        assert!(sg_describe_body(
            &svc,
            &[
                ("Filter.1.Name", "ip-permission.from-port"),
                ("Filter.1.Value.1", "22"),
            ],
        )
        .contains("<groupId>sg-1</groupId>"));
        assert!(sg_describe_body(
            &svc,
            &[
                ("Filter.1.Name", "ip-permission.protocol"),
                ("Filter.1.Value.1", "tcp"),
            ],
        )
        .contains("<groupId>sg-1</groupId>"));
        assert!(sg_describe_body(
            &svc,
            &[
                ("Filter.1.Name", "ip-permission.group-id"),
                ("Filter.1.Value.1", "sg-peer"),
            ],
        )
        .contains("<groupId>sg-1</groupId>"));
        // A CIDR not present in any rule filters the group out.
        assert!(!sg_describe_body(
            &svc,
            &[
                ("Filter.1.Name", "ip-permission.cidr"),
                ("Filter.1.Value.1", "192.168.0.0/16"),
            ],
        )
        .contains("<groupId>sg-1</groupId>"));
    }

    // ---- ValidateSecurityGroupQuotasForInterface ----

    /// Store a group carrying `ingress` inbound and `egress` outbound rules, so
    /// the quota arithmetic runs over real stored rules.
    fn seed_sized_group(svc: &Ec2Service, group_id: &str, ingress: usize, egress: usize) {
        let mut rules = Vec::new();
        for i in 0..ingress + egress {
            let mut r = ingress_rule(&format!("sgr-{group_id}-{i}"), 22, "10.0.0.0/8");
            r.group_id = group_id.to_string();
            r.is_egress = i >= ingress;
            rules.push(r);
        }
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut("000000000000", "us-east-1");
        state.security_groups.insert(
            group_id.to_string(),
            SecurityGroup {
                group_id: group_id.into(),
                group_name: group_id.into(),
                description: "d".into(),
                vpc_id: "vpc-1".into(),
                rules,
            },
        );
    }

    #[test]
    fn authorize_weighs_a_prefix_list_rule_by_its_max_entries() {
        let svc = crate::test_support::svc_enforcing_sg_quotas();
        seed_sized_group(&svc, "sg-a", 0, 0);
        {
            let mut accounts = svc.state.write();
            let state = accounts.regional_mut("000000000000", "us-east-1");
            for (id, max_entries) in [("pl-big", 61), ("pl-small", 60)] {
                state.managed_prefix_lists.insert(
                    id.to_string(),
                    crate::state::ManagedPrefixList {
                        prefix_list_id: id.into(),
                        prefix_list_name: id.into(),
                        address_family: "IPv4".into(),
                        max_entries,
                        version: 1,
                        state: "create-complete".into(),
                        state_message: None,
                        entries: Vec::new(),
                        version_history: std::collections::BTreeMap::new(),
                    },
                );
            }
        }
        let authorize_pl = |pl: &str| {
            authorize_security_group_ingress(
                &svc,
                &req(
                    "AuthorizeSecurityGroupIngress",
                    &[
                        ("GroupId", "sg-a"),
                        ("IpPermissions.1.IpProtocol", "tcp"),
                        ("IpPermissions.1.FromPort", "443"),
                        ("IpPermissions.1.ToPort", "443"),
                        ("IpPermissions.1.PrefixListIds.1.PrefixListId", pl),
                    ],
                ),
            )
        };
        // One rule, but a 61-entry list: over the default 60.
        let err = crate::test_support::err_of(authorize_pl("pl-big"));
        assert_eq!(err.code(), "RulesPerSecurityGroupLimitExceeded");
        // A 60-entry list fits exactly; nothing more IPv4 fits after it.
        assert!(authorize_pl("pl-small").is_ok());
        let err = crate::test_support::err_of(authorize_pl("pl-small"));
        assert_eq!(err.code(), "RulesPerSecurityGroupLimitExceeded");
    }

    #[test]
    fn rules_quota_is_not_checked_unless_enforced() {
        let svc = Ec2Service::new();
        seed_sized_group(
            &svc,
            "sg-a",
            crate::service::quota::DEFAULT_RULES_PER_SECURITY_GROUP,
            0,
        );
        authorize_security_group_ingress(
            &svc,
            &req(
                "AuthorizeSecurityGroupIngress",
                &[
                    ("GroupId", "sg-a"),
                    ("IpPermissions.1.IpProtocol", "tcp"),
                    ("IpPermissions.1.FromPort", "1"),
                    ("IpPermissions.1.ToPort", "1"),
                    ("IpPermissions.1.IpRanges.1.CidrIp", "192.0.2.0/24"),
                ],
            ),
        )
        .expect("the 61st rule is accepted while the quota is not enforced");
    }

    fn validate_quotas(svc: &Ec2Service, query: &[(&str, &str)]) -> String {
        let resp = validate_security_group_quotas_for_interface(
            svc,
            &req("ValidateSecurityGroupQuotasForInterface", query),
        )
        .unwrap();
        String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
    }

    #[test]
    fn validate_quotas_accepts_groups_within_the_quotas() {
        let svc = Ec2Service::new();
        seed_sized_group(
            &svc,
            "sg-a",
            crate::service::quota::DEFAULT_RULES_PER_SECURITY_GROUP,
            crate::service::quota::DEFAULT_RULES_PER_SECURITY_GROUP,
        );
        seed_sized_group(&svc, "sg-b", 1, 1);
        let body = validate_quotas(
            &svc,
            &[("SecurityGroupId.1", "sg-a"), ("SecurityGroupId.2", "sg-b")],
        );
        assert!(body.contains("<valid>true</valid>"), "{body}");
        assert!(
            body.contains("<ValidateSecurityGroupQuotasForInterfaceResponse"),
            "{body}"
        );
        assert!(body.contains("<requestId>rid</requestId>"), "{body}");
    }

    #[test]
    fn validate_quotas_unknown_group_errors() {
        let svc = Ec2Service::new();
        seed_sized_group(&svc, "sg-a", 1, 1);
        let err = crate::test_support::err_of(validate_security_group_quotas_for_interface(
            &svc,
            &req(
                "ValidateSecurityGroupQuotasForInterface",
                &[
                    ("SecurityGroupId.1", "sg-a"),
                    ("SecurityGroupId.2", "sg-gone"),
                ],
            ),
        ));
        assert_eq!(err.code(), "InvalidGroup.NotFound");
    }

    #[test]
    fn validate_quotas_requires_at_least_one_group() {
        let svc = Ec2Service::new();
        let err = crate::test_support::err_of(validate_security_group_quotas_for_interface(
            &svc,
            &req("ValidateSecurityGroupQuotasForInterface", &[]),
        ));
        assert_eq!(err.code(), "MissingParameter");
    }

    #[test]
    fn validate_quotas_rejects_a_repeated_group() {
        let svc = Ec2Service::new();
        seed_sized_group(&svc, "sg-a", 1, 1);
        let err = crate::test_support::err_of(validate_security_group_quotas_for_interface(
            &svc,
            &req(
                "ValidateSecurityGroupQuotasForInterface",
                &[("SecurityGroupId.1", "sg-a"), ("SecurityGroupId.2", "sg-a")],
            ),
        ));
        assert_eq!(err.code(), "InvalidParameterValue");
    }

    #[test]
    fn validate_quotas_rejects_too_many_groups() {
        let svc = Ec2Service::new();
        let ids: Vec<String> = (0..crate::service::quota::DEFAULT_SECURITY_GROUPS_PER_INTERFACE
            + 1)
            .map(|i| format!("sg-{i}"))
            .collect();
        for id in &ids {
            seed_sized_group(&svc, id, 1, 1);
        }
        let query: Vec<(String, String)> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| (format!("SecurityGroupId.{}", i + 1), id.clone()))
            .collect();
        let pairs: Vec<(&str, &str)> = query
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let err = crate::test_support::err_of(validate_security_group_quotas_for_interface(
            &svc,
            &req("ValidateSecurityGroupQuotasForInterface", &pairs),
        ));
        assert_eq!(err.code(), "SecurityGroupsPerInterfaceLimitExceeded");
    }

    #[test]
    fn validate_quotas_rejects_a_group_over_the_rule_limit() {
        let svc = Ec2Service::new();
        // One rule past the per-direction allowance, with the other direction
        // empty: the directions are counted separately, not summed.
        seed_sized_group(
            &svc,
            "sg-a",
            crate::service::quota::DEFAULT_RULES_PER_SECURITY_GROUP + 1,
            0,
        );
        let err = crate::test_support::err_of(validate_security_group_quotas_for_interface(
            &svc,
            &req(
                "ValidateSecurityGroupQuotasForInterface",
                &[("SecurityGroupId.1", "sg-a")],
            ),
        ));
        assert_eq!(err.code(), "RulesPerSecurityGroupLimitExceeded");
    }

    #[test]
    fn validate_quotas_dry_run_validates_without_answering() {
        let svc = Ec2Service::new();
        seed_sized_group(&svc, "sg-a", 1, 1);
        let body = validate_quotas(&svc, &[("SecurityGroupId.1", "sg-a"), ("DryRun", "true")]);
        assert!(
            !body.contains("<valid>"),
            "a dry run answers nothing: {body}"
        );

        // A dry run still reports an unknown group rather than succeeding.
        let err = crate::test_support::err_of(validate_security_group_quotas_for_interface(
            &svc,
            &req(
                "ValidateSecurityGroupQuotasForInterface",
                &[("SecurityGroupId.1", "sg-gone"), ("DryRun", "true")],
            ),
        ));
        assert_eq!(err.code(), "InvalidGroup.NotFound");
    }
}
