//! Transit Gateway core: gateways, VPC attachments, route tables, routes,
//! associations/propagations, and prefix-list references.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{ec2_arn, gen_id, indexed_list, require, validate_max_results};
use crate::state::{
    Ec2State, Tag, TgwAttachment, TgwRoute, TgwRouteTable, TransitGateway, TransitGatewayOptions,
};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

fn mr(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)
}

// ---- transit gateways ----

fn tgw_xml(t: &TransitGateway, tags: &[Tag], owner: &str, region: &str) -> String {
    let o = &t.options;
    format!(
        "{}{}{}{}{}{}<options><amazonSideAsn>{}</amazonSideAsn>\
         <autoAcceptSharedAttachments>{}</autoAcceptSharedAttachments>\
         <defaultRouteTableAssociation>{}</defaultRouteTableAssociation>\
         <defaultRouteTablePropagation>{}</defaultRouteTablePropagation>\
         <dnsSupport>{}</dnsSupport><vpnEcmpSupport>{}</vpnEcmpSupport>\
         <multicastSupport>{}</multicastSupport></options>{}",
        ec2_elem("transitGatewayId", &t.id),
        ec2_elem("state", &t.state),
        ec2_elem(
            "transitGatewayArn",
            &ec2_arn(region, owner, &format!("transit-gateway/{}", t.id))
        ),
        ec2_elem("ownerId", owner),
        ec2_elem("description", &t.description),
        ec2_elem("creationTime", FIXED_TIME),
        o.amazon_side_asn,
        o.auto_accept_shared_attachments,
        o.default_route_table_association,
        o.default_route_table_propagation,
        o.dns_support,
        o.vpn_ecmp_support,
        o.multicast_support,
        super::tags::tag_set_xml(tags),
    )
}

/// Parse the flattened `Options.*` request params into stored options, keeping
/// AWS's create-time defaults for any member the caller omits.
fn parse_tgw_options(params: &std::collections::HashMap<String, String>) -> TransitGatewayOptions {
    let mut o = TransitGatewayOptions::default();
    if let Some(v) = params
        .get("Options.AmazonSideAsn")
        .and_then(|v| v.parse().ok())
    {
        o.amazon_side_asn = v;
    }
    if let Some(v) = params.get("Options.AutoAcceptSharedAttachments") {
        o.auto_accept_shared_attachments = v.clone();
    }
    if let Some(v) = params.get("Options.DefaultRouteTableAssociation") {
        o.default_route_table_association = v.clone();
    }
    if let Some(v) = params.get("Options.DefaultRouteTablePropagation") {
        o.default_route_table_propagation = v.clone();
    }
    if let Some(v) = params.get("Options.DnsSupport") {
        o.dns_support = v.clone();
    }
    if let Some(v) = params.get("Options.VpnEcmpSupport") {
        o.vpn_ecmp_support = v.clone();
    }
    if let Some(v) = params.get("Options.MulticastSupport") {
        o.multicast_support = v.clone();
    }
    o
}

pub(crate) fn create_transit_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = gen_id("tgw");
    let t = TransitGateway {
        id: id.clone(),
        description: req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
        state: "available".to_string(),
        options: parse_tgw_options(&req.query_params),
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "transit-gateway",
        );
        let tg = state.tags_for(&id).to_vec();
        state.transit_gateways.insert(id.clone(), t.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateTransitGateway",
        &req.request_id,
        &format!(
            "<transitGateway>{}</transitGateway>",
            tgw_xml(&t, &tags, &owner, &req.region)
        ),
    ))
}

pub(crate) fn delete_transit_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let t = state
        .transit_gateways
        .remove(&id)
        .unwrap_or(TransitGateway {
            id: id.clone(),
            description: String::new(),
            state: "available".to_string(),
            options: TransitGatewayOptions::default(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteTransitGateway",
        &req.request_id,
        &format!(
            "<transitGateway>{}</transitGateway>",
            tgw_xml(&t, &tags, &owner, &req.region)
        ),
    ))
}

pub(crate) fn describe_transit_gateways(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let wanted = indexed_list(&req.query_params, "TransitGatewayId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let mut items: Vec<String> = state
        .transit_gateways
        .values()
        .filter(|t| wanted.is_empty() || wanted.contains(&t.id))
        .map(|t| tgw_xml(t, state.tags_for(&t.id), &owner, &req.region))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeTransitGateways",
        &req.request_id,
        &ec2_list("transitGatewaySet", &items),
    ))
}

pub(crate) fn modify_transit_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    if let Some(d) = req.query_params.get("Description") {
        if let Some(t) = state.transit_gateways.get_mut(&id) {
            t.description = d.clone();
        }
    }
    let t = state
        .transit_gateways
        .get(&id)
        .cloned()
        .unwrap_or(TransitGateway {
            id: id.clone(),
            description: String::new(),
            state: "available".to_string(),
            options: TransitGatewayOptions::default(),
        });
    let tags = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "ModifyTransitGateway",
        &req.request_id,
        &format!(
            "<transitGateway>{}</transitGateway>",
            tgw_xml(&t, &tags, &owner, &req.region)
        ),
    ))
}

// ---- VPC attachments ----

fn vpc_att_xml(a: &TgwAttachment, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}{}{}{}",
        ec2_elem("transitGatewayAttachmentId", &a.id),
        ec2_elem("transitGatewayId", &a.tgw_id),
        ec2_elem("vpcId", &a.resource_id),
        ec2_elem("vpcOwnerId", owner),
        ec2_elem("state", &a.state),
        ec2_list("subnetIds", &a.subnet_ids),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_transit_gateway_vpc_attachment(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let tgw_id = require(&req.query_params, "TransitGatewayId")?;
    let vpc_id = require(&req.query_params, "VpcId")?;
    let id = gen_id("tgw-attach");
    let a = TgwAttachment {
        id: id.clone(),
        tgw_id,
        resource_id: vpc_id,
        resource_type: "vpc".to_string(),
        subnet_ids: indexed_list(&req.query_params, "SubnetId"),
        state: "available".to_string(),
        nat_gateway_support: false,
        nat_system_route_support: None,
        nat_gateway_rules: Vec::new(),
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "transit-gateway-attachment",
        );
        let tg = state.tags_for(&id).to_vec();
        state.tgw_attachments.insert(id.clone(), a.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateTransitGatewayVpcAttachment",
        &req.request_id,
        &format!(
            "<transitGatewayVpcAttachment>{}</transitGatewayVpcAttachment>",
            vpc_att_xml(&a, &tags, &owner)
        ),
    ))
}

fn att_lookup(svc: &Ec2Service, req: &AwsRequest, id: &str) -> TgwAttachment {
    svc.state
        .read()
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.tgw_attachments.get(id).cloned())
        .unwrap_or(TgwAttachment {
            id: id.to_string(),
            tgw_id: "tgw-0".to_string(),
            resource_id: String::new(),
            resource_type: "vpc".to_string(),
            subnet_ids: Vec::new(),
            state: "available".to_string(),
            nat_gateway_support: false,
            nat_system_route_support: None,
            nat_gateway_rules: Vec::new(),
        })
}

pub(crate) fn delete_transit_gateway_vpc_attachment(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayAttachmentId")?;
    let owner = req.account_id.clone();
    let mut a = att_lookup(svc, req, &id);
    a.state = "deleting".to_string();
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        state.tgw_attachments.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteTransitGatewayVpcAttachment",
        &req.request_id,
        &format!(
            "<transitGatewayVpcAttachment>{}</transitGatewayVpcAttachment>",
            vpc_att_xml(&a, &[], &owner)
        ),
    ))
}

fn modify_accept_reject_vpc_att(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
    state_val: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayAttachmentId")?;
    let owner = req.account_id.clone();
    let mut a = att_lookup(svc, req, &id);
    if !state_val.is_empty() {
        a.state = state_val.to_string();
        // Persist the state transition so a later Describe* reflects it.
        let mut accounts = svc.state.write();
        if let Some(stored) = accounts
            .regional_mut(&req.account_id, &req.region)
            .tgw_attachments
            .get_mut(&id)
        {
            stored.state = state_val.to_string();
        }
    }
    Ok(Ec2Service::respond(
        action,
        &req.request_id,
        &format!(
            "<transitGatewayVpcAttachment>{}</transitGatewayVpcAttachment>",
            vpc_att_xml(&a, &[], &owner)
        ),
    ))
}

pub(crate) fn modify_transit_gateway_vpc_attachment(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayAttachmentId")?;
    let owner = req.account_id.clone();
    let add = indexed_list(&req.query_params, "AddSubnetIds");
    let remove = indexed_list(&req.query_params, "RemoveSubnetIds");
    {
        let mut accounts = svc.state.write();
        let stored = accounts
            .regional_mut(&req.account_id, &req.region)
            .tgw_attachments
            .get_mut(&id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "InvalidTransitGatewayAttachmentID.NotFound",
                    format!("Transit gateway attachment {id} was not found"),
                )
            })?;
        for s in &add {
            if !stored.subnet_ids.contains(s) {
                stored.subnet_ids.push(s.clone());
            }
        }
        stored.subnet_ids.retain(|s| !remove.contains(s));
    }
    let a = att_lookup(svc, req, &id);
    Ok(Ec2Service::respond(
        "ModifyTransitGatewayVpcAttachment",
        &req.request_id,
        &format!(
            "<transitGatewayVpcAttachment>{}</transitGatewayVpcAttachment>",
            vpc_att_xml(&a, &[], &owner)
        ),
    ))
}
pub(crate) fn accept_transit_gateway_vpc_attachment(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    modify_accept_reject_vpc_att(svc, req, "AcceptTransitGatewayVpcAttachment", "available")
}
pub(crate) fn reject_transit_gateway_vpc_attachment(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    modify_accept_reject_vpc_att(svc, req, "RejectTransitGatewayVpcAttachment", "rejected")
}

pub(crate) fn describe_transit_gateway_vpc_attachments(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let wanted = indexed_list(&req.query_params, "TransitGatewayAttachmentId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let mut items: Vec<String> = state
        .tgw_attachments
        .values()
        .filter(|a| wanted.is_empty() || wanted.contains(&a.id))
        .map(|a| vpc_att_xml(a, state.tags_for(&a.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeTransitGatewayVpcAttachments",
        &req.request_id,
        &ec2_list("transitGatewayVpcAttachments", &items),
    ))
}

pub(crate) fn describe_transit_gateway_attachments(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let mut items: Vec<String> = state
        .tgw_attachments
        .values()
        .map(|a| {
            // The route table the attachment is associated with, if any.
            let association = state
                .tgw_rt_associations
                .iter()
                .find(|(_, atts)| atts.contains(&a.id))
                .map(|(rt, _)| {
                    format!(
                        "<association>{}<state>associated</state></association>",
                        ec2_elem("transitGatewayRouteTableId", rt)
                    )
                })
                .unwrap_or_default();
            format!(
                "{}{}{}{}{}{}{}{}{}{}{}",
                ec2_elem("transitGatewayAttachmentId", &a.id),
                ec2_elem("transitGatewayId", &a.tgw_id),
                ec2_elem("transitGatewayOwnerId", &owner),
                ec2_elem("resourceOwnerId", &owner),
                ec2_elem("resourceType", &a.resource_type),
                ec2_elem("resourceId", &a.resource_id),
                ec2_elem("state", &a.state),
                association,
                ec2_elem("creationTime", FIXED_TIME),
                // The caller owns the transit gateway, so it receives this.
                ec2_elem("natGatewaySupport", nat_support_state(a)),
                super::tags::tag_set_xml(state.tags_for(&a.id)),
            )
        })
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeTransitGatewayAttachments",
        &req.request_id,
        &ec2_list("transitGatewayAttachments", &items),
    ))
}

// ---- attachment NAT configuration ----

fn nat_support_state(a: &TgwAttachment) -> &'static str {
    if a.nat_gateway_support {
        "enabled"
    } else {
        "disabled"
    }
}

/// Only Direct Connect gateway and Site-to-Site VPN attachments let the NAT
/// system routes be switched off; every other kind always installs them.
fn nat_system_route_configurable(a: &TgwAttachment) -> bool {
    matches!(a.resource_type.as_str(), "direct-connect-gateway" | "vpn")
}

fn nat_system_route_value(a: &TgwAttachment) -> &str {
    a.nat_system_route_support.as_deref().unwrap_or("enable")
}

/// The attachment's NAT configuration. `pending` overrides the support state
/// and every rule's state while a modification is being applied.
fn nat_configuration_xml(a: &TgwAttachment, pending: Option<&str>) -> String {
    let rule_state = match pending {
        Some(_) => "pending",
        None if a.nat_gateway_support => "activated",
        None => "deactivated",
    };
    let rules: Vec<String> = a
        .nat_gateway_rules
        .iter()
        .map(|r| {
            format!(
                "{}{}{}{}",
                ec2_elem("ruleNumber", &r.rule_number.to_string()),
                ec2_elem("matchSourceCidr", &r.match_source_cidr),
                ec2_elem("natCidr", &r.nat_cidr),
                ec2_elem("state", rule_state),
            )
        })
        .collect();
    format!(
        "{}{}{}{}",
        ec2_elem("transitGatewayAttachmentId", &a.id),
        ec2_elem(
            "natGatewaySupport",
            pending.unwrap_or_else(|| nat_support_state(a))
        ),
        ec2_elem("natSystemRouteSupport", nat_system_route_value(a)),
        ec2_list("natGatewayRuleSet", &rules),
    )
}

fn attachment_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "InvalidTransitGatewayAttachmentID.NotFound",
        format!("Transit gateway attachment {id} was not found"),
    )
}

fn invalid(message: impl Into<String>) -> AwsServiceError {
    crate::service_helpers::invalid_parameter_value(message)
}

/// A network-aligned IPv4 CIDR as `(network, prefix length)`.
fn strict_ipv4_cidr(member: &str, cidr: &str) -> Result<(u32, u32), AwsServiceError> {
    let parsed = crate::vpc_lookup::parse_ipv4_cidr(cidr)
        .filter(|(net, _)| {
            cidr.split_once('/')
                .and_then(|(addr, _)| addr.parse::<std::net::Ipv4Addr>().ok())
                .is_some_and(|addr| u32::from(addr) == *net)
        })
        .ok_or_else(|| invalid(format!("{member} '{cidr}' is not a valid IPv4 CIDR block")))?;
    Ok(parsed)
}

fn cidr_within((net, len): (u32, u32), (outer_net, outer_len): (u32, u32)) -> bool {
    let mask = if outer_len == 0 {
        0
    } else {
        u32::MAX << (32 - outer_len)
    };
    len >= outer_len && net & mask == outer_net
}

fn cidrs_overlap((a, la): (u32, u32), (b, lb): (u32, u32)) -> bool {
    let len = la.min(lb);
    let mask = if len == 0 { 0 } else { u32::MAX << (32 - len) };
    a & mask == b & mask
}

fn net(a: [u8; 4], len: u32) -> (u32, u32) {
    (u32::from(std::net::Ipv4Addr::from(a)), len)
}

/// Parse and validate `NatGatewayRule.N.*`: unique rule numbers in
/// `[1, 32766]`, a source CIDR outside the multicast and link-local ranges,
/// a NAT CIDR in private (RFC 1918) or shared (RFC 6598) space with the same
/// prefix length, and NAT CIDRs that do not overlap each other.
fn parse_nat_rules(
    params: &std::collections::HashMap<String, String>,
) -> Result<Vec<crate::state::TgwNatRule>, AwsServiceError> {
    let mut rules: Vec<crate::state::TgwNatRule> = Vec::new();
    let mut nat_nets: Vec<(u32, u32)> = Vec::new();
    let mut n = 1;
    while params
        .keys()
        .any(|k| k.starts_with(&format!("NatGatewayRule.{n}.")))
    {
        let key = |member: &str| format!("NatGatewayRule.{n}.{member}");
        let rule_number = require(params, &key("RuleNumber"))?
            .parse::<i64>()
            .ok()
            .filter(|r| (1..=32766).contains(r))
            .ok_or_else(|| invalid("RuleNumber must be an integer between 1 and 32766"))?;
        if rules.iter().any(|r| r.rule_number == rule_number) {
            return Err(invalid(format!(
                "Rule number {rule_number} is specified more than once"
            )));
        }
        let source = require(params, &key("MatchSourceCidr"))?;
        let nat = require(params, &key("NatCidr"))?;
        let source_net = strict_ipv4_cidr("MatchSourceCidr", &source)?;
        let nat_net = strict_ipv4_cidr("NatCidr", &nat)?;
        for reserved in [net([224, 0, 0, 0], 4), net([169, 254, 0, 0], 16)] {
            if cidrs_overlap(source_net, reserved) {
                return Err(invalid(format!(
                    "MatchSourceCidr '{source}' overlaps the multicast or link-local range"
                )));
            }
        }
        let private = [
            net([10, 0, 0, 0], 8),
            net([172, 16, 0, 0], 12),
            net([192, 168, 0, 0], 16),
            net([100, 64, 0, 0], 10),
        ];
        if !private.iter().any(|p| cidr_within(nat_net, *p)) {
            return Err(invalid(format!(
                "NatCidr '{nat}' must be in a private (RFC 1918) or shared (RFC 6598) address range"
            )));
        }
        if nat_net.1 != source_net.1 {
            return Err(invalid(format!(
                "NatCidr '{nat}' must have the same prefix length as MatchSourceCidr '{source}'"
            )));
        }
        if nat_nets.iter().any(|other| cidrs_overlap(*other, nat_net)) {
            return Err(invalid(format!(
                "NatCidr '{nat}' overlaps the NAT CIDR of another rule"
            )));
        }
        nat_nets.push(nat_net);
        rules.push(crate::state::TgwNatRule {
            rule_number,
            match_source_cidr: source,
            nat_cidr: nat,
        });
        n += 1;
    }
    if rules.len() > 50 {
        return Err(invalid("You can specify up to 50 NAT gateway rules"));
    }
    rules.sort_by_key(|r| r.rule_number);
    Ok(rules)
}

pub(crate) fn get_transit_gateway_attachment_nat_configuration(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayAttachmentId")?;
    crate::service_helpers::refuse_dry_run(&req.query_params)?;
    let accounts = svc.state.read();
    let a = accounts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.tgw_attachments.get(&id))
        .ok_or_else(|| attachment_not_found(&id))?;
    Ok(Ec2Service::respond(
        "GetTransitGatewayAttachmentNatConfiguration",
        &req.request_id,
        &nat_configuration_xml(a, None),
    ))
}

pub(crate) fn modify_transit_gateway_attachment_nat_configuration(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let params = &req.query_params;
    let id = require(params, "TransitGatewayAttachmentId")?;
    crate::service_helpers::validate_enum(params, "NatGatewaySupport", &["enable", "disable"])?;
    crate::service_helpers::validate_enum(params, "NatSystemRouteSupport", &["enable", "disable"])?;
    let reset = params
        .get("ResetNatGatewayRules")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let has_rules = params.keys().any(|k| k.starts_with("NatGatewayRule."));
    if reset && has_rules {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "InvalidParameterCombination",
            "ResetNatGatewayRules cannot be specified together with NatGatewayRules",
        ));
    }
    let new_rules = if has_rules {
        Some(parse_nat_rules(params)?)
    } else {
        None
    };
    let support = params.get("NatGatewaySupport").filter(|v| !v.is_empty());
    let system_routes = params
        .get("NatSystemRouteSupport")
        .filter(|v| !v.is_empty());

    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let a = state
        .tgw_attachments
        .get_mut(&id)
        .ok_or_else(|| attachment_not_found(&id))?;
    if let Some(v) = system_routes {
        if !nat_system_route_configurable(a) && v == "disable" {
            return Err(invalid(format!(
                "NatSystemRouteSupport cannot be disabled for {} attachments",
                a.resource_type
            )));
        }
    }
    let rules = match (&new_rules, reset) {
        (Some(r), _) => r.clone(),
        (None, true) => Vec::new(),
        (None, false) => a.nat_gateway_rules.clone(),
    };
    let enabled = match support.map(String::as_str) {
        Some("enable") => true,
        Some(_) => false,
        None => a.nat_gateway_support,
    };
    if enabled && rules.is_empty() {
        return Err(invalid(
            "NAT gateway support requires at least one NAT gateway rule",
        ));
    }
    crate::service_helpers::refuse_dry_run(params)?;
    // The change is applied asynchronously: the response reports it in
    // progress (`enabling`/`disabling`, rules `pending`), the next Get
    // reports it applied.
    let pending = if enabled != a.nat_gateway_support {
        Some(if enabled { "enabling" } else { "disabling" })
    } else if rules != a.nat_gateway_rules {
        Some(nat_support_state(a))
    } else {
        None
    };
    a.nat_gateway_support = enabled;
    a.nat_gateway_rules = rules;
    if let Some(v) = system_routes {
        a.nat_system_route_support = Some(v.clone());
    }
    let body = nat_configuration_xml(a, pending);
    Ok(Ec2Service::respond(
        "ModifyTransitGatewayAttachmentNatConfiguration",
        &req.request_id,
        &body,
    ))
}

/// The `system` routes NAT gateway support installs: each NAT CIDR of an
/// attachment with NAT gateway support enabled and NAT system routes on,
/// in every route table the attachment propagates to.
fn nat_system_routes(state: &Ec2State, route_table: &str) -> Vec<String> {
    let Some(propagating) = state.tgw_rt_propagations.get(route_table) else {
        return Vec::new();
    };
    propagating
        .iter()
        .filter_map(|att| state.tgw_attachments.get(att))
        .filter(|a| a.nat_gateway_support && nat_system_route_value(a) == "enable")
        .flat_map(|a| {
            a.nat_gateway_rules.iter().map(move |r| {
                format!(
                    "{}<type>system</type><state>active</state><transitGatewayAttachments><item>{}{}{}</item></transitGatewayAttachments>",
                    ec2_elem("destinationCidrBlock", &r.nat_cidr),
                    ec2_elem("transitGatewayAttachmentId", &a.id),
                    ec2_elem("resourceId", &a.resource_id),
                    ec2_elem("resourceType", &a.resource_type),
                )
            })
        })
        .collect()
}

// ---- route tables ----

fn rt_xml(r: &TgwRouteTable, tags: &[Tag]) -> String {
    format!(
        "{}{}<state>available</state><defaultAssociationRouteTable>false</defaultAssociationRouteTable>\
         <defaultPropagationRouteTable>false</defaultPropagationRouteTable>{}{}",
        ec2_elem("transitGatewayRouteTableId", &r.id),
        ec2_elem("transitGatewayId", &r.tgw_id),
        ec2_elem("creationTime", FIXED_TIME),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_transit_gateway_route_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let tgw_id = require(&req.query_params, "TransitGatewayId")?;
    let id = gen_id("tgw-rtb");
    let r = TgwRouteTable {
        id: id.clone(),
        tgw_id,
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "transit-gateway-route-table",
        );
        let tg = state.tags_for(&id).to_vec();
        state.tgw_route_tables.insert(id.clone(), r.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateTransitGatewayRouteTable",
        &req.request_id,
        &format!(
            "<transitGatewayRouteTable>{}</transitGatewayRouteTable>",
            rt_xml(&r, &tags)
        ),
    ))
}

pub(crate) fn delete_transit_gateway_route_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "TransitGatewayRouteTableId")?;
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    let r = state.tgw_route_tables.remove(&id).unwrap_or(TgwRouteTable {
        id: id.clone(),
        tgw_id: "tgw-0".to_string(),
    });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    state.tgw_routes.remove(&id);
    state.tgw_rt_associations.remove(&id);
    state.tgw_rt_propagations.remove(&id);
    state.tgw_prefix_list_refs.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteTransitGatewayRouteTable",
        &req.request_id,
        &format!(
            "<transitGatewayRouteTable>{}</transitGatewayRouteTable>",
            rt_xml(&r, &tags)
        ),
    ))
}

pub(crate) fn describe_transit_gateway_route_tables(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let wanted = indexed_list(&req.query_params, "TransitGatewayRouteTableId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    let mut items: Vec<String> = state
        .tgw_route_tables
        .values()
        .filter(|r| wanted.is_empty() || wanted.contains(&r.id))
        .map(|r| rt_xml(r, state.tags_for(&r.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeTransitGatewayRouteTables",
        &req.request_id,
        &ec2_list("transitGatewayRouteTables", &items),
    ))
}

fn association_xml(rt: &str, att: &str) -> String {
    format!(
        "{}{}<resourceId>vpc-0</resourceId><resourceType>vpc</resourceType><state>associated</state>",
        ec2_elem("transitGatewayRouteTableId", rt),
        ec2_elem("transitGatewayAttachmentId", att),
    )
}

pub(crate) fn associate_transit_gateway_route_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "TransitGatewayRouteTableId")?;
    let att = require(&req.query_params, "TransitGatewayAttachmentId")?;
    {
        let mut accounts = svc.state.write();
        let assocs = accounts
            .regional_mut(&req.account_id, &req.region)
            .tgw_rt_associations
            .entry(rt.clone())
            .or_default();
        if !assocs.contains(&att) {
            assocs.push(att.clone());
        }
    }
    Ok(Ec2Service::respond(
        "AssociateTransitGatewayRouteTable",
        &req.request_id,
        &format!("<association>{}</association>", association_xml(&rt, &att)),
    ))
}

pub(crate) fn disassociate_transit_gateway_route_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "TransitGatewayRouteTableId")?;
    let att = require(&req.query_params, "TransitGatewayAttachmentId")?;
    {
        let mut accounts = svc.state.write();
        if let Some(assocs) = accounts
            .regional_mut(&req.account_id, &req.region)
            .tgw_rt_associations
            .get_mut(&rt)
        {
            assocs.retain(|a| a != &att);
        }
    }
    let body = format!(
        "<association>{}{}<resourceId>vpc-0</resourceId><resourceType>vpc</resourceType><state>disassociating</state></association>",
        ec2_elem("transitGatewayRouteTableId", &rt),
        ec2_elem("transitGatewayAttachmentId", &att),
    );
    Ok(Ec2Service::respond(
        "DisassociateTransitGatewayRouteTable",
        &req.request_id,
        &body,
    ))
}

fn propagation_xml(rt: &str, att: &str, state_val: &str) -> String {
    format!(
        "{}<resourceId>vpc-0</resourceId><resourceType>vpc</resourceType>{}<state>{}</state>",
        ec2_elem("transitGatewayAttachmentId", att),
        ec2_elem("transitGatewayRouteTableId", rt),
        state_val,
    )
}

pub(crate) fn enable_transit_gateway_route_table_propagation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "TransitGatewayRouteTableId")?;
    let att = req
        .query_params
        .get("TransitGatewayAttachmentId")
        .cloned()
        .unwrap_or_else(|| "tgw-attach-0".to_string());
    {
        let mut accounts = svc.state.write();
        let props = accounts
            .regional_mut(&req.account_id, &req.region)
            .tgw_rt_propagations
            .entry(rt.clone())
            .or_default();
        if !props.contains(&att) {
            props.push(att.clone());
        }
    }
    Ok(Ec2Service::respond(
        "EnableTransitGatewayRouteTablePropagation",
        &req.request_id,
        &format!(
            "<propagation>{}</propagation>",
            propagation_xml(&rt, &att, "enabled")
        ),
    ))
}

pub(crate) fn disable_transit_gateway_route_table_propagation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "TransitGatewayRouteTableId")?;
    let att = req
        .query_params
        .get("TransitGatewayAttachmentId")
        .cloned()
        .unwrap_or_else(|| "tgw-attach-0".to_string());
    {
        let mut accounts = svc.state.write();
        if let Some(props) = accounts
            .regional_mut(&req.account_id, &req.region)
            .tgw_rt_propagations
            .get_mut(&rt)
        {
            props.retain(|a| a != &att);
        }
    }
    Ok(Ec2Service::respond(
        "DisableTransitGatewayRouteTablePropagation",
        &req.request_id,
        &format!(
            "<propagation>{}</propagation>",
            propagation_xml(&rt, &att, "disabled")
        ),
    ))
}

// ---- routes ----

fn route_xml(r: &TgwRoute) -> String {
    let atts = format!(
        "<item>{}<resourceId>vpc-0</resourceId><resourceType>vpc</resourceType></item>",
        ec2_elem("transitGatewayAttachmentId", &r.attachment_id)
    );
    format!(
        "{}<type>static</type><state>{}</state><transitGatewayAttachments>{}</transitGatewayAttachments>",
        ec2_elem("destinationCidrBlock", &r.cidr),
        r.state,
        atts,
    )
}

pub(crate) fn create_transit_gateway_route(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cidr = require(&req.query_params, "DestinationCidrBlock")?;
    let rt = require(&req.query_params, "TransitGatewayRouteTableId")?;
    let att = req
        .query_params
        .get("TransitGatewayAttachmentId")
        .cloned()
        .unwrap_or_default();
    let route = TgwRoute {
        cidr,
        attachment_id: att,
        state: "active".to_string(),
    };
    {
        let mut accounts = svc.state.write();
        accounts
            .regional_mut(&req.account_id, &req.region)
            .tgw_routes
            .entry(rt)
            .or_default()
            .push(route.clone());
    }
    Ok(Ec2Service::respond(
        "CreateTransitGatewayRoute",
        &req.request_id,
        &format!("<route>{}</route>", route_xml(&route)),
    ))
}

pub(crate) fn delete_transit_gateway_route(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "TransitGatewayRouteTableId")?;
    let cidr = require(&req.query_params, "DestinationCidrBlock")?;
    let mut route = TgwRoute {
        cidr: cidr.clone(),
        attachment_id: String::new(),
        state: "deleted".to_string(),
    };
    {
        let mut accounts = svc.state.write();
        if let Some(routes) = accounts
            .regional_mut(&req.account_id, &req.region)
            .tgw_routes
            .get_mut(&rt)
        {
            if let Some(found) = routes.iter().find(|r| r.cidr == cidr).cloned() {
                route.attachment_id = found.attachment_id;
            }
            routes.retain(|r| r.cidr != cidr);
        }
    }
    Ok(Ec2Service::respond(
        "DeleteTransitGatewayRoute",
        &req.request_id,
        &format!("<route>{}</route>", route_xml(&route)),
    ))
}

pub(crate) fn replace_transit_gateway_route(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cidr = require(&req.query_params, "DestinationCidrBlock")?;
    let rt = require(&req.query_params, "TransitGatewayRouteTableId")?;
    let att = req
        .query_params
        .get("TransitGatewayAttachmentId")
        .cloned()
        .unwrap_or_default();
    let route = TgwRoute {
        cidr: cidr.clone(),
        attachment_id: att,
        state: "active".to_string(),
    };
    {
        let mut accounts = svc.state.write();
        let routes = accounts
            .regional_mut(&req.account_id, &req.region)
            .tgw_routes
            .entry(rt)
            .or_default();
        routes.retain(|r| r.cidr != cidr);
        routes.push(route.clone());
    }
    Ok(Ec2Service::respond(
        "ReplaceTransitGatewayRoute",
        &req.request_id,
        &format!("<route>{}</route>", route_xml(&route)),
    ))
}

pub(crate) fn search_transit_gateway_routes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "TransitGatewayRouteTableId")?;
    mr(req)?;
    let accounts = svc.state.read();
    let mut items: Vec<String> = accounts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.tgw_routes.get(&rt))
        .map(|routes| routes.iter().map(route_xml).collect())
        .unwrap_or_default();
    if let Some(state) = accounts.regional(&req.account_id, &req.region) {
        items.extend(nat_system_routes(state, &rt));
    }
    let body = format!(
        "{}<additionalRoutesAvailable>false</additionalRoutesAvailable>",
        ec2_list("routeSet", &items)
    );
    Ok(Ec2Service::respond(
        "SearchTransitGatewayRoutes",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn export_transit_gateway_routes(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TransitGatewayRouteTableId")?;
    let bucket = require(&req.query_params, "S3Bucket")?;
    Ok(Ec2Service::respond(
        "ExportTransitGatewayRoutes",
        &req.request_id,
        &ec2_elem("s3Location", &format!("s3://{bucket}/tgw-routes.json")),
    ))
}

pub(crate) fn get_transit_gateway_route_table_associations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "TransitGatewayRouteTableId")?;
    mr(req)?;
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.tgw_rt_associations.get(&rt))
        .map(|atts| atts.iter().map(|att| association_xml(&rt, att)).collect())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "GetTransitGatewayRouteTableAssociations",
        &req.request_id,
        &ec2_list("associations", &items),
    ))
}

pub(crate) fn get_transit_gateway_route_table_propagations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "TransitGatewayRouteTableId")?;
    mr(req)?;
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.tgw_rt_propagations.get(&rt))
        .map(|atts| {
            atts.iter()
                .map(|att| propagation_xml(&rt, att, "enabled"))
                .collect()
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "GetTransitGatewayRouteTablePropagations",
        &req.request_id,
        &ec2_list("transitGatewayRouteTablePropagations", &items),
    ))
}

pub(crate) fn get_transit_gateway_attachment_propagations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let att = require(&req.query_params, "TransitGatewayAttachmentId")?;
    mr(req)?;
    // Surface every route table that this attachment propagates into, sorted
    // for deterministic output (the backing map iterates in arbitrary order).
    let accounts = svc.state.read();
    let mut items: Vec<String> = accounts
        .regional(&req.account_id, &req.region)
        .map(|s| {
            s.tgw_rt_propagations
                .iter()
                .filter(|(_, atts)| atts.contains(&att))
                .map(|(rt, _)| propagation_xml(rt, &att, "enabled"))
                .collect()
        })
        .unwrap_or_default();
    items.sort();
    Ok(Ec2Service::respond(
        "GetTransitGatewayAttachmentPropagations",
        &req.request_id,
        &ec2_list("transitGatewayAttachmentPropagations", &items),
    ))
}

// ---- prefix list references ----

fn plr_xml(rt: &str, pl: &str, owner: &str) -> String {
    format!(
        "{}{}{}<state>available</state><blackhole>false</blackhole>",
        ec2_elem("transitGatewayRouteTableId", rt),
        ec2_elem("prefixListId", pl),
        ec2_elem("prefixListOwnerId", owner),
    )
}

pub(crate) fn create_transit_gateway_prefix_list_reference(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    plr_cud(svc, req, "CreateTransitGatewayPrefixListReference", true)
}
pub(crate) fn modify_transit_gateway_prefix_list_reference(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    plr_cud(svc, req, "ModifyTransitGatewayPrefixListReference", true)
}
pub(crate) fn delete_transit_gateway_prefix_list_reference(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    plr_cud(svc, req, "DeleteTransitGatewayPrefixListReference", false)
}

fn plr_cud(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
    store: bool,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "TransitGatewayRouteTableId")?;
    let pl = require(&req.query_params, "PrefixListId")?;
    let owner = req.account_id.clone();
    {
        let mut accounts = svc.state.write();
        let refs = accounts
            .regional_mut(&req.account_id, &req.region)
            .tgw_prefix_list_refs
            .entry(rt.clone())
            .or_default();
        if store {
            if !refs.contains(&pl) {
                refs.push(pl.clone());
            }
        } else {
            refs.retain(|p| p != &pl);
        }
    }
    Ok(Ec2Service::respond(
        action,
        &req.request_id,
        &format!(
            "<transitGatewayPrefixListReference>{}</transitGatewayPrefixListReference>",
            plr_xml(&rt, &pl, &owner)
        ),
    ))
}

pub(crate) fn get_transit_gateway_prefix_list_references(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "TransitGatewayRouteTableId")?;
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .regional(&req.account_id, &req.region)
        .and_then(|s| s.tgw_prefix_list_refs.get(&rt))
        .map(|pls| pls.iter().map(|pl| plr_xml(&rt, pl, &owner)).collect())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "GetTransitGatewayPrefixListReferences",
        &req.request_id,
        &ec2_list("transitGatewayPrefixListReferenceSet", &items),
    ))
}

#[cfg(test)]
mod options_tests {
    use super::*;
    use crate::test_support::ec2_request as req;

    #[test]
    fn create_transit_gateway_persists_options() {
        // bug-audit 2026-07-29 (cycle 8) E2-1: CreateTransitGateway dropped the
        // whole Options block; the describe render hardcoded ASN 64512 + all
        // defaults, so a non-default value never round-tripped.
        let svc = Ec2Service::new();
        let resp = create_transit_gateway(
            &svc,
            &req(
                "CreateTransitGateway",
                &[
                    ("Options.AmazonSideAsn", "64513"),
                    ("Options.DnsSupport", "disable"),
                    ("Options.DefaultRouteTableAssociation", "disable"),
                    ("Options.MulticastSupport", "enable"),
                ],
            ),
        )
        .unwrap();
        let b = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
        let id = b
            .split("<transitGatewayId>")
            .nth(1)
            .unwrap()
            .split("</transitGatewayId>")
            .next()
            .unwrap()
            .to_string();
        let desc = String::from_utf8_lossy(
            describe_transit_gateways(&svc, &req("DescribeTransitGateways", &[]))
                .unwrap()
                .body
                .expect_bytes(),
        )
        .to_string();
        assert!(desc.contains(&id));
        assert!(
            desc.contains("<amazonSideAsn>64513</amazonSideAsn>"),
            "{desc}"
        );
        assert!(desc.contains("<dnsSupport>disable</dnsSupport>"), "{desc}");
        assert!(
            desc.contains("<defaultRouteTableAssociation>disable</defaultRouteTableAssociation>"),
            "{desc}"
        );
        assert!(
            desc.contains("<multicastSupport>enable</multicastSupport>"),
            "{desc}"
        );
    }
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

    #[test]
    fn modify_transit_gateway_vpc_attachment_persists_subnets() {
        let svc = Ec2Service::new();
        {
            let mut accounts = svc.state.write();
            accounts
                .regional_mut("000000000000", "us-east-1")
                .tgw_attachments
                .insert(
                    "tgw-attach-1".to_string(),
                    crate::state::TgwAttachment {
                        id: "tgw-attach-1".into(),
                        tgw_id: "tgw-1".into(),
                        resource_id: "vpc-1".into(),
                        resource_type: "vpc".into(),
                        subnet_ids: vec!["subnet-a".into()],
                        state: "available".into(),
                        nat_gateway_support: false,
                        nat_system_route_support: None,
                        nat_gateway_rules: Vec::new(),
                    },
                );
        }
        modify_transit_gateway_vpc_attachment(
            &svc,
            &req(
                "ModifyTransitGatewayVpcAttachment",
                &[
                    ("TransitGatewayAttachmentId", "tgw-attach-1"),
                    ("AddSubnetIds.1", "subnet-b"),
                    ("RemoveSubnetIds.1", "subnet-a"),
                ],
            ),
        )
        .unwrap();

        let accounts = svc.state.read();
        let a = &accounts
            .regional("000000000000", "us-east-1")
            .unwrap()
            .tgw_attachments["tgw-attach-1"];
        assert_eq!(a.subnet_ids, vec!["subnet-b".to_string()]);
    }

    #[test]
    fn transit_gateway_arn_uses_request_region() {
        let t = TransitGateway {
            id: "tgw-abc123".into(),
            description: String::new(),
            state: "available".into(),
            options: TransitGatewayOptions::default(),
        };
        let xml = tgw_xml(&t, &[], "000000000000", "eu-central-1");
        assert!(
            xml.contains(
                "<transitGatewayArn>arn:aws:ec2:eu-central-1:000000000000:transit-gateway/tgw-abc123</transitGatewayArn>"
            ),
            "{xml}"
        );
        assert!(!xml.contains("us-east-1"), "{xml}");
    }
}

#[cfg(test)]
mod nat_configuration_tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    fn between<'a>(xml: &'a str, tag: &str) -> &'a str {
        xml.split(&format!("<{tag}>"))
            .nth(1)
            .and_then(|s| s.split(&format!("</{tag}>")).next())
            .unwrap_or_default()
    }

    fn attachment(svc: &Ec2Service) -> String {
        let tgw = between(
            &body(create_transit_gateway(svc, &req("CreateTransitGateway", &[])).unwrap()),
            "transitGatewayId",
        )
        .to_string();
        between(
            &body(
                create_transit_gateway_vpc_attachment(
                    svc,
                    &req(
                        "CreateTransitGatewayVpcAttachment",
                        &[
                            ("TransitGatewayId", &tgw),
                            ("VpcId", "vpc-1"),
                            ("SubnetId.1", "subnet-1"),
                        ],
                    ),
                )
                .unwrap(),
            ),
            "transitGatewayAttachmentId",
        )
        .to_string()
    }

    fn modify(svc: &Ec2Service, att: &str, extra: &[(&str, &str)]) -> Result<String, String> {
        let mut q = vec![("TransitGatewayAttachmentId", att)];
        q.extend_from_slice(extra);
        modify_transit_gateway_attachment_nat_configuration(
            svc,
            &req("ModifyTransitGatewayAttachmentNatConfiguration", &q),
        )
        .map(body)
        .map_err(|e| e.code().to_string())
    }

    fn get(svc: &Ec2Service, att: &str) -> String {
        body(
            get_transit_gateway_attachment_nat_configuration(
                svc,
                &req(
                    "GetTransitGatewayAttachmentNatConfiguration",
                    &[("TransitGatewayAttachmentId", att)],
                ),
            )
            .unwrap(),
        )
    }

    const RULE: &[(&str, &str)] = &[
        ("NatGatewayRule.1.RuleNumber", "100"),
        ("NatGatewayRule.1.MatchSourceCidr", "10.0.0.0/24"),
        ("NatGatewayRule.1.NatCidr", "100.64.1.0/24"),
    ];

    #[test]
    fn rules_are_validated() {
        let svc = Ec2Service::new();
        let att = attachment(&svc);
        let bad = |rule: &[(&str, &str)]| modify(&svc, &att, rule).unwrap_err();
        let rule = |n: &str, src: &str, nat: &str| {
            vec![
                ("NatGatewayRule.1.RuleNumber", n.to_string()),
                ("NatGatewayRule.1.MatchSourceCidr", src.to_string()),
                ("NatGatewayRule.1.NatCidr", nat.to_string()),
            ]
        };
        for (n, src, nat) in [
            ("0", "10.0.0.0/24", "100.64.1.0/24"),
            ("32767", "10.0.0.0/24", "100.64.1.0/24"),
            ("1", "224.0.0.0/24", "100.64.1.0/24"),
            ("1", "169.254.1.0/24", "100.64.1.0/24"),
            ("1", "10.0.0.0/24", "8.8.8.0/24"),
            ("1", "10.0.0.0/24", "100.64.1.0/25"),
            ("1", "10.0.0.1/24", "100.64.1.0/24"),
        ] {
            let r = rule(n, src, nat);
            let r: Vec<(&str, &str)> = r.iter().map(|(k, v)| (*k, v.as_str())).collect();
            assert_eq!(bad(&r), "InvalidParameterValue", "{n} {src} {nat}");
        }
        // NAT CIDRs of one attachment may not overlap, nor rule numbers repeat.
        let mut two = RULE.to_vec();
        two.extend([
            ("NatGatewayRule.2.RuleNumber", "200"),
            ("NatGatewayRule.2.MatchSourceCidr", "10.0.1.0/25"),
            ("NatGatewayRule.2.NatCidr", "100.64.1.0/25"),
        ]);
        assert_eq!(bad(&two), "InvalidParameterValue");
        two[3].1 = "100";
        two[5].1 = "100.64.2.0/25";
        assert_eq!(bad(&two), "InvalidParameterValue");
        // Rules and a reset are exclusive; enabling needs a rule.
        let mut reset = RULE.to_vec();
        reset.push(("ResetNatGatewayRules", "true"));
        assert_eq!(bad(&reset), "InvalidParameterCombination");
        assert_eq!(
            bad(&[("NatGatewaySupport", "enable")]),
            "InvalidParameterValue"
        );
        // A VPC attachment always installs its NAT routes.
        assert_eq!(
            bad(&[("NatSystemRouteSupport", "disable")]),
            "InvalidParameterValue"
        );
        assert_eq!(
            err_of(get_transit_gateway_attachment_nat_configuration(
                &svc,
                &req(
                    "GetTransitGatewayAttachmentNatConfiguration",
                    &[("TransitGatewayAttachmentId", "tgw-attach-missing")],
                ),
            ))
            .code(),
            "InvalidTransitGatewayAttachmentID.NotFound"
        );
    }

    #[test]
    fn enabling_nat_installs_system_routes_where_the_attachment_propagates() {
        let svc = Ec2Service::new();
        let att = attachment(&svc);
        let rt = "tgw-rtb-1";
        enable_transit_gateway_route_table_propagation(
            &svc,
            &req(
                "EnableTransitGatewayRouteTablePropagation",
                &[
                    ("TransitGatewayRouteTableId", rt),
                    ("TransitGatewayAttachmentId", &att),
                ],
            ),
        )
        .unwrap();
        let routes = || {
            body(
                search_transit_gateway_routes(
                    &svc,
                    &req(
                        "SearchTransitGatewayRoutes",
                        &[("TransitGatewayRouteTableId", rt)],
                    ),
                )
                .unwrap(),
            )
        };

        // Rules alone change nothing while support is off.
        let staged = modify(&svc, &att, RULE).unwrap();
        assert_eq!(between(&staged, "natGatewaySupport"), "disabled");
        assert!(staged.contains("<state>pending</state>"), "{staged}");
        let settled = get(&svc, &att);
        assert!(settled.contains("<state>deactivated</state>"), "{settled}");
        assert!(!routes().contains("<type>system</type>"));

        let enabling = modify(&svc, &att, &[("NatGatewaySupport", "enable")]).unwrap();
        assert_eq!(between(&enabling, "natGatewaySupport"), "enabling");
        let settled = get(&svc, &att);
        assert_eq!(between(&settled, "natGatewaySupport"), "enabled");
        assert_eq!(between(&settled, "ruleNumber"), "100");
        assert!(settled.contains("<state>activated</state>"), "{settled}");
        let r = routes();
        assert!(r.contains("<type>system</type>"), "{r}");
        assert_eq!(between(&r, "destinationCidrBlock"), "100.64.1.0/24");

        // DescribeTransitGatewayAttachments reports the support state.
        let described = body(
            describe_transit_gateway_attachments(
                &svc,
                &req("DescribeTransitGatewayAttachments", &[]),
            )
            .unwrap(),
        );
        assert_eq!(between(&described, "natGatewaySupport"), "enabled");

        // A reset needs support switched off in the same request.
        assert_eq!(
            modify(&svc, &att, &[("ResetNatGatewayRules", "true")]).unwrap_err(),
            "InvalidParameterValue"
        );
        let off = modify(
            &svc,
            &att,
            &[
                ("ResetNatGatewayRules", "true"),
                ("NatGatewaySupport", "disable"),
            ],
        )
        .unwrap();
        assert_eq!(between(&off, "natGatewaySupport"), "disabling");
        let settled = get(&svc, &att);
        assert_eq!(between(&settled, "natGatewaySupport"), "disabled");
        assert!(!settled.contains("<ruleNumber>"), "{settled}");
        assert!(!routes().contains("<type>system</type>"));
    }
}
