//! EC2 traffic mirror operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

fn traffic_mirror_filter_rule_xml(r: &TrafficMirrorFilterRule) -> String {
    let port_range = |name: &str, range: Option<(i64, i64)>| {
        range
            .map(|(f, t)| format!("<{name}><fromPort>{f}</fromPort><toPort>{t}</toPort></{name}>"))
            .unwrap_or_default()
    };
    format!(
        "{}{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("trafficMirrorFilterRuleId", &r.id),
        ec2_elem("trafficMirrorFilterId", &r.filter_id),
        ec2_elem("trafficDirection", &r.traffic_direction),
        ec2_elem("ruleNumber", &r.rule_number.to_string()),
        ec2_elem("ruleAction", &r.rule_action),
        ec2_elem_opt("protocol", r.protocol.map(|p| p.to_string()).as_deref()),
        port_range("destinationPortRange", r.destination_port_range),
        port_range("sourcePortRange", r.source_port_range),
        ec2_elem_opt("destinationCidrBlock", r.destination_cidr_block.as_deref()),
        ec2_elem_opt("sourceCidrBlock", r.source_cidr_block.as_deref()),
        ec2_elem_opt("description", r.description.as_deref()),
    )
}

fn traffic_mirror_filter_xml(
    f: &TrafficMirrorFilter,
    rules: &[&TrafficMirrorFilterRule],
    tags: &[Tag],
) -> String {
    let ingress: Vec<String> = rules
        .iter()
        .filter(|r| r.traffic_direction == "ingress")
        .map(|r| traffic_mirror_filter_rule_xml(r))
        .collect();
    let egress: Vec<String> = rules
        .iter()
        .filter(|r| r.traffic_direction == "egress")
        .map(|r| traffic_mirror_filter_rule_xml(r))
        .collect();
    format!(
        "{}{}{}{}{}{}",
        ec2_elem("trafficMirrorFilterId", &f.id),
        ec2_list("ingressFilterRuleSet", &ingress),
        ec2_list("egressFilterRuleSet", &egress),
        fakecloud_aws::ec2query::ec2_scalar_list("networkServiceSet", &f.network_services),
        ec2_elem_opt("description", f.description.as_deref()),
        super::super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_traffic_mirror_filter(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = gen_id("tmf");
    let f = TrafficMirrorFilter {
        id: id.clone(),
        description: req
            .query_params
            .get("Description")
            .filter(|v| !v.is_empty())
            .cloned(),
        network_services: Vec::new(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "traffic-mirror-filter",
        );
        let tg = state.tags_for(&id).to_vec();
        state.traffic_mirror_filters.insert(id.clone(), f.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateTrafficMirrorFilter",
        &req.request_id,
        &format!(
            "<trafficMirrorFilter>{}</trafficMirrorFilter>",
            traffic_mirror_filter_xml(&f, &[], &tags)
        ),
    ))
}

pub(crate) fn create_traffic_mirror_filter_rule(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let filter_id = require(&req.query_params, "TrafficMirrorFilterId")?;
    let traffic_direction = require(&req.query_params, "TrafficDirection")?;
    let rule_number = require(&req.query_params, "RuleNumber")?
        .parse::<i64>()
        .map_err(|_| {
            crate::service_helpers::invalid_parameter_value("RuleNumber must be an integer")
        })?;
    let rule_action = require(&req.query_params, "RuleAction")?;
    let destination_cidr_block = Some(require(&req.query_params, "DestinationCidrBlock")?);
    let source_cidr_block = Some(require(&req.query_params, "SourceCidrBlock")?);
    validate_enum(
        &req.query_params,
        "TrafficDirection",
        &["ingress", "egress"],
    )?;
    validate_enum(&req.query_params, "RuleAction", &["accept", "reject"])?;
    let id = gen_id("tmfr");
    let r = TrafficMirrorFilterRule {
        id: id.clone(),
        filter_id,
        traffic_direction,
        rule_number,
        rule_action,
        protocol: req
            .query_params
            .get("Protocol")
            .and_then(|v| v.parse::<i64>().ok()),
        destination_cidr_block,
        source_cidr_block,
        destination_port_range: parse_port_range(req, "DestinationPortRange"),
        source_port_range: parse_port_range(req, "SourcePortRange"),
        description: req
            .query_params
            .get("Description")
            .filter(|v| !v.is_empty())
            .cloned(),
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .traffic_mirror_filter_rules
            .insert(id.clone(), r.clone());
    }
    Ok(Ec2Service::respond(
        "CreateTrafficMirrorFilterRule",
        &req.request_id,
        &format!(
            "<trafficMirrorFilterRule>{}</trafficMirrorFilterRule>",
            traffic_mirror_filter_rule_xml(&r)
        ),
    ))
}

fn traffic_mirror_session_xml(s: &TrafficMirrorSession, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("trafficMirrorSessionId", &s.id),
        ec2_elem("trafficMirrorTargetId", &s.target_id),
        ec2_elem("trafficMirrorFilterId", &s.filter_id),
        ec2_elem("networkInterfaceId", &s.network_interface_id),
        ec2_elem("ownerId", owner),
        ec2_elem_opt(
            "packetLength",
            s.packet_length.map(|p| p.to_string()).as_deref()
        ),
        ec2_elem("sessionNumber", &s.session_number.to_string()),
        ec2_elem_opt(
            "virtualNetworkId",
            s.virtual_network_id.map(|v| v.to_string()).as_deref()
        ),
        ec2_elem_opt("description", s.description.as_deref()),
        super::super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_traffic_mirror_session(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let network_interface_id = require(&req.query_params, "NetworkInterfaceId")?;
    let target_id = require(&req.query_params, "TrafficMirrorTargetId")?;
    let filter_id = require(&req.query_params, "TrafficMirrorFilterId")?;
    let session_number = require(&req.query_params, "SessionNumber")?
        .parse::<i64>()
        .map_err(|_| {
            crate::service_helpers::invalid_parameter_value("SessionNumber must be an integer")
        })?;
    let id = gen_id("tms");
    let owner = req.account_id.clone();
    let s = TrafficMirrorSession {
        id: id.clone(),
        target_id,
        filter_id,
        network_interface_id,
        packet_length: req
            .query_params
            .get("PacketLength")
            .and_then(|v| v.parse::<i64>().ok()),
        session_number,
        virtual_network_id: req
            .query_params
            .get("VirtualNetworkId")
            .and_then(|v| v.parse::<i64>().ok()),
        description: req
            .query_params
            .get("Description")
            .filter(|v| !v.is_empty())
            .cloned(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "traffic-mirror-session",
        );
        let tg = state.tags_for(&id).to_vec();
        state.traffic_mirror_sessions.insert(id.clone(), s.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateTrafficMirrorSession",
        &req.request_id,
        &format!(
            "<trafficMirrorSession>{}</trafficMirrorSession>",
            traffic_mirror_session_xml(&s, &tags, &owner)
        ),
    ))
}

fn traffic_mirror_target_xml(t: &TrafficMirrorTarget, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}{}{}{}",
        ec2_elem("trafficMirrorTargetId", &t.id),
        ec2_elem_opt("networkInterfaceId", t.network_interface_id.as_deref()),
        ec2_elem_opt(
            "networkLoadBalancerArn",
            t.network_load_balancer_arn.as_deref()
        ),
        ec2_elem("type", &t.target_type),
        ec2_elem_opt("description", t.description.as_deref()),
        ec2_elem("ownerId", owner),
        super::super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_traffic_mirror_target(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let network_interface_id = req
        .query_params
        .get("NetworkInterfaceId")
        .filter(|v| !v.is_empty())
        .cloned();
    let network_load_balancer_arn = req
        .query_params
        .get("NetworkLoadBalancerArn")
        .filter(|v| !v.is_empty())
        .cloned();
    let gateway_lb_endpoint_id = req
        .query_params
        .get("GatewayLoadBalancerEndpointId")
        .filter(|v| !v.is_empty())
        .cloned();
    let target_type = if network_interface_id.is_some() {
        "network-interface"
    } else if network_load_balancer_arn.is_some() {
        "network-load-balancer"
    } else {
        "gateway-load-balancer-endpoint"
    }
    .to_string();
    let id = gen_id("tmt");
    let owner = req.account_id.clone();
    let t = TrafficMirrorTarget {
        id: id.clone(),
        network_interface_id,
        network_load_balancer_arn,
        gateway_lb_endpoint_id,
        target_type,
        description: req
            .query_params
            .get("Description")
            .filter(|v| !v.is_empty())
            .cloned(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "traffic-mirror-target",
        );
        let tg = state.tags_for(&id).to_vec();
        state.traffic_mirror_targets.insert(id.clone(), t.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateTrafficMirrorTarget",
        &req.request_id,
        &format!(
            "<trafficMirrorTarget>{}</trafficMirrorTarget>",
            traffic_mirror_target_xml(&t, &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_traffic_mirror_filter(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorFilterId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let existed = state.traffic_mirror_filters.remove(&id).is_some();
    if existed {
        state.tags.remove(&id);
        state
            .traffic_mirror_filter_rules
            .retain(|_, r| r.filter_id != id);
    }
    Ok(Ec2Service::respond(
        "DeleteTrafficMirrorFilter",
        &req.request_id,
        &ec2_elem("trafficMirrorFilterId", &id),
    ))
}

pub(crate) fn delete_traffic_mirror_filter_rule(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorFilterRuleId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    state.traffic_mirror_filter_rules.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteTrafficMirrorFilterRule",
        &req.request_id,
        &ec2_elem("trafficMirrorFilterRuleId", &id),
    ))
}

pub(crate) fn delete_traffic_mirror_session(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorSessionId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let existed = state.traffic_mirror_sessions.remove(&id).is_some();
    if existed {
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteTrafficMirrorSession",
        &req.request_id,
        &ec2_elem("trafficMirrorSessionId", &id),
    ))
}

pub(crate) fn delete_traffic_mirror_target(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorTargetId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let existed = state.traffic_mirror_targets.remove(&id).is_some();
    if existed {
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteTrafficMirrorTarget",
        &req.request_id,
        &ec2_elem("trafficMirrorTargetId", &id),
    ))
}

pub(crate) fn describe_traffic_mirror_filter_rules(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "TrafficMirrorFilterRuleId");
    let filter_id = req
        .query_params
        .get("TrafficMirrorFilterId")
        .filter(|v| !v.is_empty());
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .traffic_mirror_filter_rules
        .values()
        .filter(|r| wanted.is_empty() || wanted.contains(&r.id))
        .filter(|r| filter_id.is_none_or(|f| &r.filter_id == f))
        .map(traffic_mirror_filter_rule_xml)
        .collect();
    Ok(Ec2Service::respond(
        "DescribeTrafficMirrorFilterRules",
        &req.request_id,
        &ec2_list("trafficMirrorFilterRuleSet", &items),
    ))
}

pub(crate) fn describe_traffic_mirror_filters(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "TrafficMirrorFilterId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .traffic_mirror_filters
        .values()
        .filter(|f| wanted.is_empty() || wanted.contains(&f.id))
        .map(|f| {
            let rules: Vec<&TrafficMirrorFilterRule> = state
                .traffic_mirror_filter_rules
                .values()
                .filter(|r| r.filter_id == f.id)
                .collect();
            traffic_mirror_filter_xml(f, &rules, state.tags_for(&f.id))
        })
        .collect();
    Ok(Ec2Service::respond(
        "DescribeTrafficMirrorFilters",
        &req.request_id,
        &ec2_list("trafficMirrorFilterSet", &items),
    ))
}

pub(crate) fn describe_traffic_mirror_sessions(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "TrafficMirrorSessionId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .traffic_mirror_sessions
        .values()
        .filter(|s| wanted.is_empty() || wanted.contains(&s.id))
        .map(|s| traffic_mirror_session_xml(s, state.tags_for(&s.id), &owner))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeTrafficMirrorSessions",
        &req.request_id,
        &ec2_list("trafficMirrorSessionSet", &items),
    ))
}

pub(crate) fn describe_traffic_mirror_targets(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "TrafficMirrorTargetId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .traffic_mirror_targets
        .values()
        .filter(|t| wanted.is_empty() || wanted.contains(&t.id))
        .map(|t| traffic_mirror_target_xml(t, state.tags_for(&t.id), &owner))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeTrafficMirrorTargets",
        &req.request_id,
        &ec2_list("trafficMirrorTargetSet", &items),
    ))
}

pub(crate) fn modify_traffic_mirror_filter_network_services(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorFilterId")?;
    let add = indexed_list(&req.query_params, "AddNetworkService");
    let remove = indexed_list(&req.query_params, "RemoveNetworkService");
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Mutate when present; synthesize for a probe-only synthetic id.
    let mut synth = TrafficMirrorFilter {
        id: id.clone(),
        description: None,
        network_services: Vec::new(),
    };
    let entry = state
        .traffic_mirror_filters
        .get_mut(&id)
        .unwrap_or(&mut synth);
    entry.network_services.retain(|s| !remove.contains(s));
    for s in add {
        if !entry.network_services.contains(&s) {
            entry.network_services.push(s);
        }
    }
    let f = entry.clone();
    let tags = state.tags_for(&id).to_vec();
    let rules: Vec<&TrafficMirrorFilterRule> = state
        .traffic_mirror_filter_rules
        .values()
        .filter(|r| r.filter_id == id)
        .collect();
    Ok(Ec2Service::respond(
        "ModifyTrafficMirrorFilterNetworkServices",
        &req.request_id,
        &format!(
            "<trafficMirrorFilter>{}</trafficMirrorFilter>",
            traffic_mirror_filter_xml(&f, &rules, &tags)
        ),
    ))
}

pub(crate) fn modify_traffic_mirror_filter_rule(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorFilterRuleId")?;
    validate_enum(
        &req.query_params,
        "TrafficDirection",
        &["ingress", "egress"],
    )?;
    validate_enum(&req.query_params, "RuleAction", &["accept", "reject"])?;
    let remove_fields = indexed_list(&req.query_params, "RemoveField");
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Mutate when present; synthesize for a probe-only synthetic id.
    let mut synth = TrafficMirrorFilterRule {
        id: id.clone(),
        filter_id: req
            .query_params
            .get("TrafficMirrorFilterId")
            .cloned()
            .unwrap_or_default(),
        traffic_direction: "ingress".to_string(),
        rule_number: 0,
        rule_action: "accept".to_string(),
        protocol: None,
        destination_cidr_block: None,
        source_cidr_block: None,
        destination_port_range: None,
        source_port_range: None,
        description: None,
    };
    let entry = state
        .traffic_mirror_filter_rules
        .get_mut(&id)
        .unwrap_or(&mut synth);
    if let Some(d) = req.query_params.get("TrafficDirection") {
        entry.traffic_direction = d.clone();
    }
    if let Some(n) = req
        .query_params
        .get("RuleNumber")
        .and_then(|v| v.parse::<i64>().ok())
    {
        entry.rule_number = n;
    }
    if let Some(a) = req.query_params.get("RuleAction") {
        entry.rule_action = a.clone();
    }
    if let Some(p) = req
        .query_params
        .get("Protocol")
        .and_then(|v| v.parse::<i64>().ok())
    {
        entry.protocol = Some(p);
    }
    if let Some(c) = req.query_params.get("DestinationCidrBlock") {
        entry.destination_cidr_block = Some(c.clone());
    }
    if let Some(c) = req.query_params.get("SourceCidrBlock") {
        entry.source_cidr_block = Some(c.clone());
    }
    if let Some(pr) = parse_port_range(req, "DestinationPortRange") {
        entry.destination_port_range = Some(pr);
    }
    if let Some(pr) = parse_port_range(req, "SourcePortRange") {
        entry.source_port_range = Some(pr);
    }
    if let Some(d) = req.query_params.get("Description") {
        entry.description = Some(d.clone());
    }
    // RemoveField clears optional members back to unset.
    for field in &remove_fields {
        match field.as_str() {
            "destination-port-range" => entry.destination_port_range = None,
            "source-port-range" => entry.source_port_range = None,
            "protocol" => entry.protocol = None,
            "description" => entry.description = None,
            _ => {}
        }
    }
    let r = entry.clone();
    Ok(Ec2Service::respond(
        "ModifyTrafficMirrorFilterRule",
        &req.request_id,
        &format!(
            "<trafficMirrorFilterRule>{}</trafficMirrorFilterRule>",
            traffic_mirror_filter_rule_xml(&r)
        ),
    ))
}

pub(crate) fn modify_traffic_mirror_session(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TrafficMirrorSessionId")?;
    let owner = req.account_id.clone();
    let remove_fields = indexed_list(&req.query_params, "RemoveField");
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Mutate when present; synthesize for a probe-only synthetic id.
    let mut synth = TrafficMirrorSession {
        id: id.clone(),
        target_id: req
            .query_params
            .get("TrafficMirrorTargetId")
            .cloned()
            .unwrap_or_default(),
        filter_id: req
            .query_params
            .get("TrafficMirrorFilterId")
            .cloned()
            .unwrap_or_default(),
        network_interface_id: String::new(),
        packet_length: None,
        session_number: 0,
        virtual_network_id: None,
        description: None,
    };
    let entry = state
        .traffic_mirror_sessions
        .get_mut(&id)
        .unwrap_or(&mut synth);
    if let Some(t) = req.query_params.get("TrafficMirrorTargetId") {
        entry.target_id = t.clone();
    }
    if let Some(f) = req.query_params.get("TrafficMirrorFilterId") {
        entry.filter_id = f.clone();
    }
    if let Some(n) = req
        .query_params
        .get("SessionNumber")
        .and_then(|v| v.parse::<i64>().ok())
    {
        entry.session_number = n;
    }
    if let Some(p) = req
        .query_params
        .get("PacketLength")
        .and_then(|v| v.parse::<i64>().ok())
    {
        entry.packet_length = Some(p);
    }
    if let Some(v) = req
        .query_params
        .get("VirtualNetworkId")
        .and_then(|v| v.parse::<i64>().ok())
    {
        entry.virtual_network_id = Some(v);
    }
    if let Some(d) = req.query_params.get("Description") {
        entry.description = Some(d.clone());
    }
    for field in &remove_fields {
        match field.as_str() {
            "packet-length" => entry.packet_length = None,
            "virtual-network-id" => entry.virtual_network_id = None,
            "description" => entry.description = None,
            _ => {}
        }
    }
    let s = entry.clone();
    let tags = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "ModifyTrafficMirrorSession",
        &req.request_id,
        &format!(
            "<trafficMirrorSession>{}</trafficMirrorSession>",
            traffic_mirror_session_xml(&s, &tags, &owner)
        ),
    ))
}
