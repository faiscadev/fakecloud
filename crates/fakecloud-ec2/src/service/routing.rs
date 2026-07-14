//! Route tables, internet gateways, NAT gateways, and egress-only IGWs.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    filter_value_matches, gen_id, indexed_list, parse_filters, require, validate_enum,
    validate_int_range, validate_max_results, Filter,
};
use crate::state::{
    Ec2State, InternetGateway, NatGateway, Route, RouteTable, RouteTableAssociation, Tag,
};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

// ---- rendering ----

fn route_xml(r: &Route) -> String {
    let mut out = String::new();
    let opt = |out: &mut String, name: &str, v: &Option<String>| {
        if let Some(v) = v {
            out.push_str(&ec2_elem(name, v));
        }
    };
    opt(&mut out, "destinationCidrBlock", &r.destination_cidr_block);
    opt(
        &mut out,
        "destinationIpv6CidrBlock",
        &r.destination_ipv6_cidr_block,
    );
    opt(
        &mut out,
        "destinationPrefixListId",
        &r.destination_prefix_list_id,
    );
    opt(&mut out, "gatewayId", &r.gateway_id);
    opt(&mut out, "natGatewayId", &r.nat_gateway_id);
    opt(&mut out, "networkInterfaceId", &r.network_interface_id);
    opt(&mut out, "instanceId", &r.instance_id);
    opt(
        &mut out,
        "vpcPeeringConnectionId",
        &r.vpc_peering_connection_id,
    );
    opt(&mut out, "transitGatewayId", &r.transit_gateway_id);
    opt(
        &mut out,
        "egressOnlyInternetGatewayId",
        &r.egress_only_internet_gateway_id,
    );
    out.push_str(&ec2_elem("state", &r.state));
    out.push_str(&ec2_elem("origin", &r.origin));
    out
}

fn rt_xml(rt: &RouteTable, tags: &[Tag], owner: &str) -> String {
    let routes: Vec<String> = rt.routes.iter().map(route_xml).collect();
    let assocs: Vec<String> = rt
        .associations
        .iter()
        .map(|a| {
            let mut s = format!(
                "{}{}<main>{}</main><associationState><state>associated</state></associationState>",
                ec2_elem("routeTableAssociationId", &a.association_id),
                ec2_elem("routeTableId", &a.route_table_id),
                a.main,
            );
            if let Some(sn) = &a.subnet_id {
                s.push_str(&ec2_elem("subnetId", sn));
            }
            if let Some(gw) = &a.gateway_id {
                s.push_str(&ec2_elem("gatewayId", gw));
            }
            s
        })
        .collect();
    format!(
        "{}{}{}{}{}{}",
        ec2_elem("routeTableId", &rt.route_table_id),
        ec2_elem("vpcId", &rt.vpc_id),
        ec2_elem("ownerId", owner),
        ec2_list("routeSet", &routes),
        ec2_list("associationSet", &assocs),
        super::tags::tag_set_xml(tags),
    )
}

fn igw_xml(igw: &InternetGateway, tags: &[Tag], owner: &str) -> String {
    let atts: Vec<String> = igw
        .attachments
        .iter()
        .map(|(vpc, state)| format!("{}{}", ec2_elem("vpcId", vpc), ec2_elem("state", state)))
        .collect();
    format!(
        "{}{}{}{}",
        ec2_elem("internetGatewayId", &igw.internet_gateway_id),
        ec2_elem("ownerId", owner),
        ec2_list("attachmentSet", &atts),
        super::tags::tag_set_xml(tags),
    )
}

fn eigw_xml(igw: &InternetGateway, tags: &[Tag]) -> String {
    let atts: Vec<String> = igw
        .attachments
        .iter()
        .map(|(vpc, state)| format!("{}{}", ec2_elem("vpcId", vpc), ec2_elem("state", state)))
        .collect();
    format!(
        "{}{}{}",
        ec2_elem("egressOnlyInternetGatewayId", &igw.internet_gateway_id),
        ec2_list("attachmentSet", &atts),
        super::tags::tag_set_xml(tags),
    )
}

fn nat_address_xml(allocation_id: &Option<String>) -> String {
    let mut s = format!(
        "{}<isPrimary>true</isPrimary>{}{}",
        ec2_elem("privateIp", "10.0.0.10"),
        ec2_elem("networkInterfaceId", "eni-00000000000000000"),
        ec2_elem("status", "succeeded"),
    );
    if let Some(a) = allocation_id {
        s.push_str(&ec2_elem("allocationId", a));
        s.push_str(&ec2_elem("publicIp", "52.0.0.10"));
        s.push_str(&ec2_elem("associationId", "eipassoc-00000000000000000"));
    }
    s
}

fn nat_xml(n: &NatGateway, tags: &[Tag]) -> String {
    let addrs = vec![nat_address_xml(&n.allocation_id)];
    format!(
        "{}{}{}{}{}{}{}{}",
        ec2_elem("natGatewayId", &n.nat_gateway_id),
        ec2_elem("subnetId", &n.subnet_id),
        ec2_elem("vpcId", &n.vpc_id),
        ec2_elem("state", &n.state),
        ec2_elem("connectivityType", &n.connectivity_type),
        ec2_elem("createTime", FIXED_TIME),
        ec2_list("natGatewayAddressSet", &addrs),
        super::tags::tag_set_xml(tags),
    )
}

// ---- route tables ----

fn parse_route(params: &std::collections::HashMap<String, String>) -> Route {
    let g = |k: &str| params.get(k).cloned();
    Route {
        destination_cidr_block: g("DestinationCidrBlock"),
        destination_ipv6_cidr_block: g("DestinationIpv6CidrBlock"),
        destination_prefix_list_id: g("DestinationPrefixListId"),
        gateway_id: g("GatewayId"),
        nat_gateway_id: g("NatGatewayId"),
        network_interface_id: g("NetworkInterfaceId"),
        instance_id: g("InstanceId"),
        vpc_peering_connection_id: g("VpcPeeringConnectionId"),
        transit_gateway_id: g("TransitGatewayId"),
        egress_only_internet_gateway_id: g("EgressOnlyInternetGatewayId"),
        state: "active".to_string(),
        origin: "CreateRoute".to_string(),
    }
}

pub(crate) fn create_route_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc_id = require(&req.query_params, "VpcId")?;
    let id = gen_id("rtb");
    let rt = RouteTable {
        route_table_id: id.clone(),
        vpc_id,
        routes: vec![Route {
            destination_cidr_block: Some("10.0.0.0/16".to_string()),
            gateway_id: Some("local".to_string()),
            state: "active".to_string(),
            origin: "CreateRouteTable".to_string(),
            ..Default::default()
        }],
        associations: Vec::new(),
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "route-table",
        );
        let t = state.tags_for(&id).to_vec();
        state.route_tables.insert(id.clone(), rt.clone());
        t
    };
    let body = format!(
        "<routeTable>{}</routeTable>{}",
        rt_xml(&rt, &tags, &owner),
        ec2_elem("clientToken", &gen_id("token")),
    );
    Ok(Ec2Service::respond(
        "CreateRouteTable",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_route_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "RouteTableId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    state.route_tables.remove(&id);
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteRouteTable",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_route_tables(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 100)?;
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "RouteTableId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .route_tables
        .values()
        .filter(|rt| wanted.is_empty() || wanted.contains(&rt.route_table_id))
        .filter(|rt| {
            // Synthesize `association.main` so the provider's main-route-table
            // lookup (a singular find) resolves to exactly the main RT instead
            // of every RT in the VPC.
            let main = if rt.associations.iter().any(|a| a.main) {
                "true"
            } else {
                "false"
            };
            let subnet_assoc: Vec<&str> = rt
                .associations
                .iter()
                .filter_map(|a| a.subnet_id.as_deref())
                .collect();
            let assoc_ids: Vec<&str> = rt
                .associations
                .iter()
                .map(|a| a.association_id.as_str())
                .collect();
            simple_match_multi(
                &[
                    ("route-table-id", &rt.route_table_id),
                    ("vpc-id", &rt.vpc_id),
                    ("association.main", main),
                ],
                &[
                    ("association.subnet-id", &subnet_assoc),
                    ("association.route-table-association-id", &assoc_ids),
                ],
                state.tags_for(&rt.route_table_id),
                &filters,
            )
        })
        .map(|rt| rt_xml(rt, state.tags_for(&rt.route_table_id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeRouteTables",
        &req.request_id,
        &ec2_list("routeTableSet", &items),
    ))
}

fn route_mutate(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
    replace: bool,
) -> Result<AwsResponse, AwsServiceError> {
    let rt_id = require(&req.query_params, "RouteTableId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(rt) = state.route_tables.get_mut(&rt_id) {
            let route = parse_route(&req.query_params);
            if replace {
                if let Some(existing) = rt
                    .routes
                    .iter_mut()
                    .find(|r| r.destination_cidr_block == route.destination_cidr_block)
                {
                    *existing = route;
                } else {
                    rt.routes.push(route);
                }
            } else {
                rt.routes.push(route);
            }
        }
    }
    Ok(Ec2Service::respond(
        action,
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn create_route(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    route_mutate(svc, req, "CreateRoute", false)
}

pub(crate) fn replace_route(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    route_mutate(svc, req, "ReplaceRoute", true)
}

pub(crate) fn delete_route(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt_id = require(&req.query_params, "RouteTableId")?;
    let cidr = req.query_params.get("DestinationCidrBlock").cloned();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(rt) = state.route_tables.get_mut(&rt_id) {
            rt.routes
                .retain(|r| r.destination_cidr_block != cidr || cidr.is_none());
        }
    }
    Ok(Ec2Service::respond(
        "DeleteRoute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn associate_route_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt_id = require(&req.query_params, "RouteTableId")?;
    let assoc_id = gen_id("rtbassoc");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(rt) = state.route_tables.get_mut(&rt_id) {
            rt.associations.push(RouteTableAssociation {
                association_id: assoc_id.clone(),
                route_table_id: rt_id.clone(),
                subnet_id: req.query_params.get("SubnetId").cloned(),
                gateway_id: req.query_params.get("GatewayId").cloned(),
                main: false,
            });
        }
    }
    let body = format!(
        "{}<associationState><state>associated</state></associationState>",
        ec2_elem("associationId", &assoc_id)
    );
    Ok(Ec2Service::respond(
        "AssociateRouteTable",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn disassociate_route_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let assoc_id = require(&req.query_params, "AssociationId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for rt in state.route_tables.values_mut() {
            rt.associations.retain(|a| a.association_id != assoc_id);
        }
    }
    Ok(Ec2Service::respond(
        "DisassociateRouteTable",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn replace_route_table_association(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let assoc_id = require(&req.query_params, "AssociationId")?;
    let rt_id = require(&req.query_params, "RouteTableId")?;
    let new_id = gen_id("rtbassoc");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // Detach the association from whichever route table currently owns it,
        // preserving whether it's the main association or a subnet association.
        let mut moved: Option<crate::state::RouteTableAssociation> = None;
        for rt in state.route_tables.values_mut() {
            if let Some(pos) = rt
                .associations
                .iter()
                .position(|a| a.association_id == assoc_id)
            {
                moved = Some(rt.associations.remove(pos));
                break;
            }
        }
        // Re-attach it to the target route table with a fresh id. Moving the
        // association (rather than rewriting its fields in place) is what lets a
        // `main` association actually migrate to the new table, so the
        // DescribeRouteTables `association.main` lookup resolves the new table.
        if let Some(mut assoc) = moved {
            assoc.association_id = new_id.clone();
            assoc.route_table_id = rt_id.clone();
            if let Some(target) = state.route_tables.get_mut(&rt_id) {
                target.associations.push(assoc);
            }
        }
    }
    let body = format!(
        "{}<associationState><state>associated</state></associationState>",
        ec2_elem("newAssociationId", &new_id)
    );
    Ok(Ec2Service::respond(
        "ReplaceRouteTableAssociation",
        &req.request_id,
        &body,
    ))
}

// ---- internet gateways ----

pub(crate) fn create_internet_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = gen_id("igw");
    let igw = InternetGateway {
        internet_gateway_id: id.clone(),
        attachments: Vec::new(),
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "internet-gateway",
        );
        let t = state.tags_for(&id).to_vec();
        state.internet_gateways.insert(id.clone(), igw.clone());
        t
    };
    let body = format!(
        "<internetGateway>{}</internetGateway>",
        igw_xml(&igw, &tags, &owner)
    );
    Ok(Ec2Service::respond(
        "CreateInternetGateway",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_internet_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InternetGatewayId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    state.internet_gateways.remove(&id);
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteInternetGateway",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_internet_gateways(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "InternetGatewayId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .internet_gateways
        .values()
        .filter(|g| wanted.is_empty() || wanted.contains(&g.internet_gateway_id))
        .map(|g| igw_xml(g, state.tags_for(&g.internet_gateway_id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeInternetGateways",
        &req.request_id,
        &ec2_list("internetGatewaySet", &items),
    ))
}

pub(crate) fn attach_internet_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InternetGatewayId")?;
    let vpc = require(&req.query_params, "VpcId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(g) = state.internet_gateways.get_mut(&id) {
            g.attachments.push((vpc, "available".to_string()));
        }
    }
    Ok(Ec2Service::respond(
        "AttachInternetGateway",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn detach_internet_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InternetGatewayId")?;
    let vpc = require(&req.query_params, "VpcId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(g) = state.internet_gateways.get_mut(&id) {
            g.attachments.retain(|(v, _)| v != &vpc);
        }
    }
    Ok(Ec2Service::respond(
        "DetachInternetGateway",
        &req.request_id,
        &ec2_return(true),
    ))
}

// ---- egress-only internet gateways ----

pub(crate) fn create_egress_only_igw(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc = require(&req.query_params, "VpcId")?;
    let id = gen_id("eigw");
    let igw = InternetGateway {
        internet_gateway_id: id.clone(),
        attachments: vec![(vpc, "attached".to_string())],
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "egress-only-internet-gateway",
        );
        let t = state.tags_for(&id).to_vec();
        state.egress_only_igws.insert(id.clone(), igw.clone());
        t
    };
    let body = format!(
        "<egressOnlyInternetGateway>{}</egressOnlyInternetGateway>{}",
        eigw_xml(&igw, &tags),
        ec2_elem("clientToken", &gen_id("token")),
    );
    Ok(Ec2Service::respond(
        "CreateEgressOnlyInternetGateway",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_egress_only_igw(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "EgressOnlyInternetGatewayId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.egress_only_igws.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteEgressOnlyInternetGateway",
        &req.request_id,
        "<returnCode>true</returnCode>",
    ))
}

pub(crate) fn describe_egress_only_igws(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 255)?;
    let wanted = indexed_list(&req.query_params, "EgressOnlyInternetGatewayId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .egress_only_igws
        .values()
        .filter(|g| wanted.is_empty() || wanted.contains(&g.internet_gateway_id))
        .map(|g| eigw_xml(g, state.tags_for(&g.internet_gateway_id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeEgressOnlyInternetGateways",
        &req.request_id,
        &ec2_list("egressOnlyInternetGatewaySet", &items),
    ))
}

// ---- NAT gateways ----

pub(crate) fn create_nat_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(
        &req.query_params,
        "ConnectivityType",
        &["private", "public"],
    )?;
    validate_enum(
        &req.query_params,
        "AvailabilityMode",
        &["zonal", "regional"],
    )?;
    validate_int_range(&req.query_params, "SecondaryPrivateIpAddressCount", 1, 31)?;
    let id = gen_id("nat");
    let nat = NatGateway {
        nat_gateway_id: id.clone(),
        subnet_id: req
            .query_params
            .get("SubnetId")
            .cloned()
            .unwrap_or_default(),
        vpc_id: String::new(),
        state: "available".to_string(),
        connectivity_type: req
            .query_params
            .get("ConnectivityType")
            .cloned()
            .unwrap_or_else(|| "public".to_string()),
        allocation_id: req.query_params.get("AllocationId").cloned(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &id, "natgateway");
        let t = state.tags_for(&id).to_vec();
        state.nat_gateways.insert(id.clone(), nat.clone());
        t
    };
    let body = format!(
        "<natGateway>{}</natGateway>{}",
        nat_xml(&nat, &tags),
        ec2_elem("clientToken", &gen_id("token")),
    );
    Ok(Ec2Service::respond(
        "CreateNatGateway",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_nat_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NatGatewayId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.nat_gateways.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteNatGateway",
        &req.request_id,
        &ec2_elem("natGatewayId", &id),
    ))
}

pub(crate) fn describe_nat_gateways(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "NatGatewayId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .nat_gateways
        .values()
        .filter(|n| wanted.is_empty() || wanted.contains(&n.nat_gateway_id))
        .map(|n| nat_xml(n, state.tags_for(&n.nat_gateway_id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeNatGateways",
        &req.request_id,
        &ec2_list("natGatewaySet", &items),
    ))
}

fn nat_address_op(req: &AwsRequest, action: &str) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "NatGatewayId")?;
    validate_int_range(&req.query_params, "PrivateIpAddressCount", 1, 31)?;
    validate_int_range(&req.query_params, "MaxDrainDurationSeconds", 1, 4000)?;
    let body = format!(
        "{}{}",
        ec2_elem("natGatewayId", &id),
        ec2_list(
            "natGatewayAddressSet",
            &[nat_address_xml(&Some("eipalloc-0".to_string()))]
        ),
    );
    Ok(Ec2Service::respond(action, &req.request_id, &body))
}

pub(crate) fn assign_private_nat_gateway_address(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    nat_address_op(req, "AssignPrivateNatGatewayAddress")
}

pub(crate) fn associate_nat_gateway_address(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    nat_address_op(req, "AssociateNatGatewayAddress")
}

pub(crate) fn disassociate_nat_gateway_address(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    nat_address_op(req, "DisassociateNatGatewayAddress")
}

pub(crate) fn unassign_private_nat_gateway_address(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    nat_address_op(req, "UnassignPrivateNatGatewayAddress")
}

// ---- shared filter ----

/// Filter matcher for route-table describe. Accepts both single-valued fields
/// and multi-valued fields (a single filter name backed by a list of candidate
/// values, e.g. `association.subnet-id` over a route table's several subnet
/// associations).
fn simple_match_multi(
    fields: &[(&str, &str)],
    multi_fields: &[(&str, &[&str])],
    tags: &[Tag],
    filters: &[Filter],
) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> =
            if let Some((_, v)) = fields.iter().find(|(n, _)| *n == f.name) {
                vec![v.to_string()]
            } else if let Some((_, vs)) = multi_fields.iter().find(|(n, _)| *n == f.name) {
                vs.iter().map(|s| s.to_string()).collect()
            } else if f.name == "tag-key" {
                tags.iter().map(|t| t.key.clone()).collect()
            } else if let Some(key) = f.name.strip_prefix("tag:") {
                tags.iter()
                    .filter(|t| t.key == key)
                    .map(|t| t.value.clone())
                    .collect()
            } else {
                return false;
            };
        f.values
            .iter()
            .any(|v| candidates.iter().any(|c| filter_value_matches(v, c)))
    })
}
